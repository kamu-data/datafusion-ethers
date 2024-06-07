use alloy::{
    providers::RootProvider,
    rpc::types::eth::{BlockNumberOrTag, Filter},
};
use datafusion_ethers::stream::{StreamOptions, StreamState};
use futures::TryStreamExt as _;

///////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_stream_raw_logs() {
    let test_chain = super::chain::get_test_chain().await;

    let assert_stream = |rpc_client: RootProvider<_>,
                         filter: Filter,
                         options: StreamOptions,
                         resume_from_state: Option<StreamState>,
                         expected_batches: Vec<(usize, StreamState)>| async move {
        let mut stream = Box::pin(datafusion_ethers::stream::RawLogsStream::paginate(
            rpc_client,
            filter,
            options,
            resume_from_state,
        ));

        let mut actual_batches = Vec::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            actual_batches.push((batch.logs.len(), batch.state));
        }

        assert_eq!(actual_batches, expected_batches);
    };

    // Full scan with one request
    assert_stream(
        test_chain.rpc_client.clone(),
        Filter::default()
            .from_block(BlockNumberOrTag::Earliest)
            .to_block(BlockNumberOrTag::Latest),
        StreamOptions::default(),
        None,
        vec![(4, StreamState { last_seen_block: 4 })],
    )
    .await;

    // Limited block range
    assert_stream(
        test_chain.rpc_client.clone(),
        Filter::default()
            .from_block(BlockNumberOrTag::Earliest)
            .to_block(3),
        StreamOptions::default(),
        None,
        vec![(2, StreamState { last_seen_block: 3 })],
    )
    .await;

    // Limited block range to one block
    assert_stream(
        test_chain.rpc_client.clone(),
        Filter::default().from_block(4).to_block(4),
        StreamOptions::default(),
        None,
        vec![(2, StreamState { last_seen_block: 4 })],
    )
    .await;

    // Scan one block at a time
    assert_stream(
        test_chain.rpc_client.clone(),
        Filter::default()
            .from_block(BlockNumberOrTag::Earliest)
            .to_block(BlockNumberOrTag::Latest),
        StreamOptions { block_stride: 1 },
        None,
        vec![
            (0, StreamState { last_seen_block: 0 }),
            (0, StreamState { last_seen_block: 1 }),
            (0, StreamState { last_seen_block: 2 }),
            (2, StreamState { last_seen_block: 3 }),
            (2, StreamState { last_seen_block: 4 }),
        ],
    )
    .await;

    // Resume scanning
    assert_stream(
        test_chain.rpc_client.clone(),
        Filter::default()
            .from_block(BlockNumberOrTag::Earliest)
            .to_block(BlockNumberOrTag::Latest),
        StreamOptions { block_stride: 1 },
        Some(StreamState { last_seen_block: 3 }),
        vec![(2, StreamState { last_seen_block: 4 })],
    )
    .await;
}

///////////////////////////////////////////////////////////////////////////////////////////////////
