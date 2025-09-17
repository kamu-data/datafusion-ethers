use alloy::{
    providers::{DynProvider, Provider},
    rpc::types::eth::{BlockNumberOrTag, Filter, FilterBlockOption, Log},
    transports::{RpcError, TransportErrorKind},
};

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct StreamOptions {
    /// Number of blocks to scan in one query. This should be small, as many
    /// RPC providers impose limits on this parameter
    pub block_stride: u64,

    /// Many providers don't yet return `blockTimestamp` from `eth_getLogs` RPC endpoint
    /// and in such cases `block_timestamp` column will be `null`.
    /// If you enable this fallback the library will perform additional call to `eth_getBlock`
    /// to populate the timestam, but this may result in significant performance penalty when
    /// fetching many log records.
    ///
    /// See: https://github.com/ethereum/execution-apis/issues/295
    pub use_block_timestamp_fallback: bool,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            block_stride: 10_000,
            use_block_timestamp_fallback: false,
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
    pub fn paginate(
        rpc_client: DynProvider<alloy::network::AnyNetwork>,
        mut filter: Filter,
        options: StreamOptions,
        resume_from_state: Option<StreamState>,
    ) -> impl futures::Stream<Item = Result<StreamBatch, RpcError<TransportErrorKind>>> {
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
                let mut logs = rpc_client.get_logs(&filter).await?;

                // Resolve timestamp if needed
                if options.use_block_timestamp_fallback && logs.iter().any(|l| l.block_timestamp.is_none()) {
                    Self::populate_block_timestamps_fallback(&rpc_client, &mut logs).await?;
                }

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

    /// This function will determine the timestamps for the first and last block in the batch
    /// and approximates the timestamps for blocks in between by interpolating.
    /// NOTE: This might not be reproducible if stride changes between runs
    async fn populate_block_timestamps_fallback(
        rpc_client: &DynProvider<alloy::network::AnyNetwork>,
        logs: &mut [Log],
    ) -> Result<(), RpcError<TransportErrorKind>> {
        let first_log = logs.first().unwrap();
        let last_log = logs.last().unwrap();

        let first_block = rpc_client
            .get_block_by_hash(first_log.block_hash.unwrap())
            .await?
            .unwrap();

        let last_block = if first_log.block_hash == last_log.block_hash {
            first_block.clone()
        } else {
            rpc_client
                .get_block_by_hash(last_log.block_hash.unwrap())
                .await?
                .unwrap()
        };

        let time_per_block = ((last_block.header.timestamp - first_block.header.timestamp) as f64)
            / ((last_block.number() - first_block.number()) as f64);

        tracing::debug!(
            block_range = ?(first_block.number(), last_block.number()),
            time_per_block,
            "Populating block timestamps via fallback mechanism",
        );

        for log in logs.iter_mut() {
            let block_offset = log.block_number.unwrap() - first_block.number();
            let time_offset = time_per_block * (block_offset as f64);
            let ts = first_block.header.timestamp + (time_offset.floor() as u64);
            log.block_timestamp = Some(ts);
        }

        Ok(())
    }

    pub async fn filter_to_block_range(
        rpc_client: &DynProvider<alloy::network::AnyNetwork>,
        block_option: &FilterBlockOption,
    ) -> Result<(u64, u64), RpcError<TransportErrorKind>> {
        match block_option {
            FilterBlockOption::Range {
                from_block: Some(from),
                to_block: Some(to),
            } => {
                let from = match from {
                    BlockNumberOrTag::Earliest => 0,
                    BlockNumberOrTag::Number(n) => *n,
                    _ => Err(RpcError::local_usage_str(&format!(
                        "Invalid range: {block_option:?}"
                    )))?,
                };
                let to = match to {
                    BlockNumberOrTag::Number(n) => *n,
                    BlockNumberOrTag::Latest
                    | BlockNumberOrTag::Safe
                    | BlockNumberOrTag::Finalized => {
                        let Some(to_block) = rpc_client.get_block((*to).into()).await? else {
                            Err(RpcError::local_usage_str(&format!(
                                "Unable to resolve block: {to:?}"
                            )))?
                        };
                        to_block.header.number
                    }
                    _ => Err(RpcError::local_usage_str(&format!(
                        "Invalid range: {block_option:?}"
                    )))?,
                };
                Ok((from, to))
            }
            FilterBlockOption::Range { .. } => Err(RpcError::local_usage_str(&format!(
                "Invalid range: {block_option:?}"
            )))?,
            FilterBlockOption::AtBlockHash(_) => {
                unimplemented!("Querying a single block by hash is not yet supported")
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
