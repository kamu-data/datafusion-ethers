use std::sync::Arc;

use ethers::prelude::*;

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct StreamOptions {
    /// Number of blocks to scan in one query. This should be small, as many
    /// RPC providers impose limits on this parameter
    pub block_stride: u64,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            block_stride: 10_000,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamState {
    pub last_seen_block: u64,
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub struct StreamBatch {
    pub logs: Vec<Log>,
    pub state: StreamState,
    pub block_range_all: (u64, u64),
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RawLogsStream;

impl RawLogsStream {
    // TODO: Re-org detection and handling
    /// Streams batches of raw logs efficient and resumable pagination over `eth_getLogs` RPC endpoint,
    pub fn paginate<P: JsonRpcClient>(
        rpc_client: Arc<Provider<P>>,
        mut filter: Filter,
        options: StreamOptions,
        resume_from_state: Option<StreamState>,
    ) -> impl futures::Stream<Item = Result<StreamBatch, ProviderError>> {
        async_stream::try_stream! {
            // Determine query's full block range, resolving symbolic block aliases like
            // 'latest' and 'finalized' to block numbers
            let block_range_all = Self::filter_to_block_range(&rpc_client, &filter.block_option).await?;

            // Subtract from the full block range the range that was already processed
            let block_range_unprocessed = if let Some(last_seen_block) = resume_from_state.map(|s| s.last_seen_block) {
                (
                    u64::max(last_seen_block + 1, block_range_all.0),
                    block_range_all.1,
                )
            } else {
                block_range_all
            };

            tracing::info!(
                block_range_query = ?filter.block_option,
                ?block_range_all,
                ?block_range_unprocessed,
                "Computed block ranges",
            );

            let mut block_range_to_scan = block_range_unprocessed;

            while block_range_to_scan.0 <= block_range_to_scan.1 {
                let block_range_page = (
                    block_range_to_scan.0,
                    u64::min(
                        block_range_to_scan.1,
                        block_range_to_scan.0 + options.block_stride - 1,
                    ),
                );

                // Setup per-query filter
                filter.block_option = FilterBlockOption::Range {
                    from_block: Some(block_range_page.0.into()),
                    to_block: Some(block_range_page.1.into()),
                };

                tracing::debug!(
                    ?block_range_page,
                    "Querying block range",
                );

                // Query the node
                let logs = rpc_client.get_logs(&filter).await?;

                yield StreamBatch {
                    logs,
                    state: StreamState { last_seen_block: block_range_page.1 },
                    block_range_all,
                };

                // Update remaining range
                block_range_to_scan.0 = block_range_page.1 + 1;
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////

    pub async fn filter_to_block_range<P: JsonRpcClient>(
        rpc_client: &Arc<Provider<P>>,
        block_option: &FilterBlockOption,
    ) -> Result<(u64, u64), ProviderError> {
        match block_option {
            FilterBlockOption::Range {
                from_block: Some(from),
                to_block: Some(to),
            } => {
                let from = match from {
                    BlockNumber::Earliest => 0,
                    BlockNumber::Number(n) => n.as_u64(),
                    _ => Err(ProviderError::CustomError(format!(
                        "Invalid range: {block_option:?}"
                    )))?,
                };
                let to = match to {
                    BlockNumber::Number(n) => n.as_u64(),
                    BlockNumber::Latest | BlockNumber::Safe | BlockNumber::Finalized => {
                        let Some(to_block) = rpc_client.get_block(*to).await? else {
                            Err(ProviderError::CustomError(format!(
                                "Unable to resolve block: {to:?}"
                            )))?
                        };
                        to_block.number.unwrap().as_u64()
                    }
                    _ => Err(ProviderError::CustomError(format!(
                        "Invalid range: {block_option:?}"
                    )))?,
                };
                Ok((from, to))
            }
            FilterBlockOption::Range { .. } => Err(ProviderError::CustomError(format!(
                "Invalid range: {block_option:?}"
            )))?,
            FilterBlockOption::AtBlockHash(_) => {
                unimplemented!("Querying a single block by hash is not yet supported")
            }
        }
    }
}
