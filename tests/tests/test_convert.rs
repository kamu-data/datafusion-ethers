use std::sync::Arc;

use alloy_core::hex;
use alloy_core::sol_types::SolEvent;
use datafusion::prelude::*;
use datafusion_ethers::convert::Transcoder as _;
use ethers::prelude::*;
use indoc::indoc;

///////////////////////////////////////////////////////////////////////////////////////////////////

alloy_core::sol! {
    event SendRequest(uint64 indexed requestId, address indexed consumerAddr, bytes request);
}

///////////////////////////////////////////////////////////////////////////////////////////////////

fn get_sample_log() -> Log {
    let event = SendRequest {
        requestId: 123,
        consumerAddr: "aabbccddaabbccddaabbccddaabbccddaabbccdd".parse().unwrap(),
        request: hex!("ff00bbaa").into(),
    };

    let data = event.encode_data();
    let topics = event.encode_topics();
    Log {
        address: "bbccddaabbccddaabbccddaabbccddaabbccddaa".parse().unwrap(),
        topics: topics
            .into_iter()
            .map(|v| H256::from_slice(v.as_slice()))
            .collect(),
        data: data.into(),
        block_hash: Some(
            "2cde5a35148e10cdaf096c8037f1f472bb48cc41cb2b9ae91bbce02a8171f503"
                .parse()
                .unwrap(),
        ),
        block_number: Some(5798139.into()),
        transaction_hash: Some(
            "455a23fae16392028366f6d3cf3ef3cbaa1a90a70bfccfa4ae263d1a382cb992"
                .parse()
                .unwrap(),
        ),
        transaction_index: Some(46.into()),
        log_index: Some(84.into()),
        transaction_log_index: None,
        log_type: None,
        removed: None,
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_raw_logs_to_record_batch() {
    let mut coder = datafusion_ethers::convert::EthRawLogsToArrow::new();
    coder.append(&[get_sample_log()]).unwrap();
    let batch = coder.finish();

    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch).unwrap();

    super::utils::assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 block_number (INTEGER(64,false));
              REQUIRED BYTE_ARRAY block_hash;
              REQUIRED INT64 transaction_index (INTEGER(64,false));
              REQUIRED BYTE_ARRAY transaction_hash;
              REQUIRED INT64 log_index (INTEGER(64,false));
              REQUIRED BYTE_ARRAY address;
              OPTIONAL BYTE_ARRAY topic0;
              OPTIONAL BYTE_ARRAY topic1;
              OPTIONAL BYTE_ARRAY topic2;
              OPTIONAL BYTE_ARRAY topic3;
              REQUIRED BYTE_ARRAY data;
            }
            "#
        ),
    );

    super::utils::assert_data_eq(
        df,
        indoc!(
            r#"
            +--------------+------------------------------------------------------------------+-------------------+------------------------------------------------------------------+-----------+------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            | block_number | block_hash                                                       | transaction_index | transaction_hash                                                 | log_index | address                                  | topic0                                                           | topic1                                                           | topic2                                                           | topic3 | data                                                                                                                                                                                             |
            +--------------+------------------------------------------------------------------+-------------------+------------------------------------------------------------------+-----------+------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            | 5798139      | 2cde5a35148e10cdaf096c8037f1f472bb48cc41cb2b9ae91bbce02a8171f503 | 46                | 455a23fae16392028366f6d3cf3ef3cbaa1a90a70bfccfa4ae263d1a382cb992 | 84        | bbccddaabbccddaabbccddaabbccddaabbccddaa | 41987d99f799d840cf38d453e305eb131a47bb34369b3a78d7177de3c2659af5 | 000000000000000000000000000000000000000000000000000000000000007b | 000000000000000000000000aabbccddaabbccddaabbccddaabbccddaabbccdd |        | 00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000004ff00bbaa00000000000000000000000000000000000000000000000000000000 |
            +--------------+------------------------------------------------------------------+-------------------+------------------------------------------------------------------+-----------+------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            "#
        ),
    )
    .await;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_decoded_logs_to_record_batch() {
    let event = alloy_core::json_abi::Event::parse(
        "event SendRequest(uint64 indexed requestId, address indexed consumerAddr, bytes request)",
    )
    .unwrap();

    let mut coder = datafusion_ethers::convert::EthDecodedLogsToArrow::new(&event);
    coder.append(&[get_sample_log()]).unwrap();
    let batch = coder.finish();

    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch).unwrap();

    super::utils::assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 requestId (INTEGER(64,false));
              REQUIRED FIXED_LEN_BYTE_ARRAY (20) consumerAddr;
              REQUIRED BYTE_ARRAY request;
            }
            "#
        ),
    );

    super::utils::assert_data_eq(
        df,
        indoc!(
            r#"
            +-----------+------------------------------------------+----------+
            | requestId | consumerAddr                             | request  |
            +-----------+------------------------------------------+----------+
            | 123       | aabbccddaabbccddaabbccddaabbccddaabbccdd | ff00bbaa |
            +-----------+------------------------------------------+----------+
            "#
        ),
    )
    .await;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_raw_and_decoded_logs_to_record_batch() {
    let event = alloy_core::json_abi::Event::parse(
        "event SendRequest(uint64 indexed requestId, address indexed consumerAddr, bytes request)",
    )
    .unwrap();

    let mut coder = datafusion_ethers::convert::EthRawAndDecodedLogsToArrow::new(&event);
    coder.append(&[get_sample_log()]).unwrap();
    let batch = coder.finish();

    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch).unwrap();

    super::utils::assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 block_number (INTEGER(64,false));
              REQUIRED BYTE_ARRAY block_hash;
              REQUIRED INT64 transaction_index (INTEGER(64,false));
              REQUIRED BYTE_ARRAY transaction_hash;
              REQUIRED INT64 log_index (INTEGER(64,false));
              REQUIRED BYTE_ARRAY address;
              OPTIONAL BYTE_ARRAY topic0;
              OPTIONAL BYTE_ARRAY topic1;
              OPTIONAL BYTE_ARRAY topic2;
              OPTIONAL BYTE_ARRAY topic3;
              REQUIRED BYTE_ARRAY data;
              REQUIRED group event {
                REQUIRED INT64 requestId (INTEGER(64,false));
                REQUIRED FIXED_LEN_BYTE_ARRAY (20) consumerAddr;
                REQUIRED BYTE_ARRAY request;
              }
            }
            "#
        ),
    );

    super::utils::assert_data_eq(
        df,
        indoc!(
            r#"
            +--------------+------------------------------------------------------------------+-------------------+------------------------------------------------------------------+-----------+------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------+
            | block_number | block_hash                                                       | transaction_index | transaction_hash                                                 | log_index | address                                  | topic0                                                           | topic1                                                           | topic2                                                           | topic3 | data                                                                                                                                                                                             | event                                                                                       |
            +--------------+------------------------------------------------------------------+-------------------+------------------------------------------------------------------+-----------+------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------+
            | 5798139      | 2cde5a35148e10cdaf096c8037f1f472bb48cc41cb2b9ae91bbce02a8171f503 | 46                | 455a23fae16392028366f6d3cf3ef3cbaa1a90a70bfccfa4ae263d1a382cb992 | 84        | bbccddaabbccddaabbccddaabbccddaabbccddaa | 41987d99f799d840cf38d453e305eb131a47bb34369b3a78d7177de3c2659af5 | 000000000000000000000000000000000000000000000000000000000000007b | 000000000000000000000000aabbccddaabbccddaabbccddaabbccddaabbccdd |        | 00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000004ff00bbaa00000000000000000000000000000000000000000000000000000000 | {requestId: 123, consumerAddr: aabbccddaabbccddaabbccddaabbccddaabbccdd, request: ff00bbaa} |
            +--------------+------------------------------------------------------------------+-------------------+------------------------------------------------------------------+-----------+------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------+
            "#
        ),
    )
    .await;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_udf_eth_decode_event() {
    let mut ctx = SessionContext::new();
    datafusion_functions_json::register_all(&mut ctx).unwrap();
    datafusion_ethers::udf::register_all(&mut ctx).unwrap();

    let mut coder = datafusion_ethers::convert::EthRawLogsToArrow::new();
    coder.append(&[get_sample_log()]).unwrap();
    let batch = coder.finish();
    ctx.register_batch("logs", batch).unwrap();

    // Success
    let df = ctx
            .sql(
                r#"
                select
                    eth_decode_event(
                        'SendRequest(uint64 indexed requestId, address indexed consumerAddr, bytes request)',
                        topic0,
                        topic1,
                        topic2,
                        topic3,
                        data
                    ) as event
                from logs
                "#,
            )
            .await
            .unwrap();

    super::utils::assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              OPTIONAL BYTE_ARRAY event (STRING);
            }
            "#
        ),
    );

    super::utils::assert_data_eq(
        df,
        indoc!(
            r#"
            +-----------------------------------------------------------------------------------------------------------------------+
            | event                                                                                                                 |
            +-----------------------------------------------------------------------------------------------------------------------+
            | {"consumerAddr":"aabbccddaabbccddaabbccddaabbccddaabbccdd","name":"SendRequest","request":"ff00bbaa","requestId":123} |
            +-----------------------------------------------------------------------------------------------------------------------+
            "#
        ),
    ).await;

    // Fail
    let df = ctx
        .sql(
            r#"
                select
                    eth_try_decode_event(
                        'SendRequest(address indexed consumerAddr, bytes request)',
                        topic0,
                        topic1,
                        topic2,
                        topic3,
                        data
                    ) as event
                from logs
                "#,
        )
        .await
        .unwrap();

    super::utils::assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              OPTIONAL BYTE_ARRAY event (STRING);
            }
            "#
        ),
    );

    super::utils::assert_data_eq(
        df,
        indoc!(
            r#"
            +-------+
            | event |
            +-------+
            |       |
            +-------+
            "#
        ),
    )
    .await;

    let df = ctx
            .sql(
                r#"
                select
                    json_get_str(event, 'name') as name,
                    json_get_int(event, 'requestId') as request_id,
                    decode(json_get_str(event, 'consumerAddr'), 'hex') as consumer_addr,
                    decode(json_get_str(event, 'request'), 'hex') as request
                from (
                    select
                        eth_decode_event(
                            'SendRequest(uint64 indexed requestId, address indexed consumerAddr, bytes request)',
                            topic0,
                            topic1,
                            topic2,
                            topic3,
                            data
                        ) as event
                    from logs
                )
                "#,
            )
            .await
            .unwrap();

    super::utils::assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              OPTIONAL BYTE_ARRAY name (STRING);
              OPTIONAL INT64 request_id;
              OPTIONAL BYTE_ARRAY consumer_addr;
              OPTIONAL BYTE_ARRAY request;
            }
            "#
        ),
    );

    super::utils::assert_data_eq(
        df,
        indoc!(
            r#"
            +-------------+------------+------------------------------------------+----------+
            | name        | request_id | consumer_addr                            | request  |
            +-------------+------------+------------------------------------------+----------+
            | SendRequest | 123        | aabbccddaabbccddaabbccddaabbccddaabbccdd | ff00bbaa |
            +-------------+------------+------------------------------------------+----------+
            "#
        ),
    )
    .await;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_udf_eth_event_selector() {
    let mut ctx = SessionContext::new();
    datafusion_functions_json::register_all(&mut ctx).unwrap();
    datafusion_ethers::udf::register_all(&mut ctx).unwrap();

    let df = ctx
            .sql(
                r#"
                select eth_event_selector(
                    'SendRequest(uint64 indexed requestId, address indexed consumerAddr, bytes request)'
                ) as selector
                "#,
            )
            .await
            .unwrap();

    super::utils::assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              OPTIONAL BYTE_ARRAY selector;
            }
            "#
        ),
    );

    super::utils::assert_data_eq(
        df,
        indoc!(
            r#"
            +------------------------------------------------------------------+
            | selector                                                         |
            +------------------------------------------------------------------+
            | 41987d99f799d840cf38d453e305eb131a47bb34369b3a78d7177de3c2659af5 |
            +------------------------------------------------------------------+
            "#
        ),
    )
    .await;

    assert_eq!(
        get_sample_log().topics[0].as_bytes(),
        hex!("41987d99f799d840cf38d453e305eb131a47bb34369b3a78d7177de3c2659af5"),
    );
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sql_to_pushdown_filter() {
    let rpc_client = Arc::new(Provider::<Http>::connect("http://localhost:12345").await);

    let df_ctx = SessionContext::new();
    df_ctx.register_catalog(
        "eth",
        Arc::new(datafusion_ethers::provider::EthCatalog::new(rpc_client)),
    );

    // ---
    let filter =
        datafusion_ethers::convert::sql_to_pushdown_filter(&df_ctx, "select * from eth.eth.logs")
            .await
            .unwrap()
            .unwrap();
    assert!(
        matches!(
            filter.block_option,
            FilterBlockOption::Range {
                from_block: Some(BlockNumber::Earliest),
                to_block: Some(BlockNumber::Latest),
            }
        ),
        "{:?}",
        filter
    );
    assert!(matches!(filter.address, None), "{:?}", filter);
    assert!(
        matches!(filter.topics, [None, None, None, None]),
        "{:?}",
        filter
    );

    // ---
    let filter = datafusion_ethers::convert::sql_to_pushdown_filter(
        &df_ctx,
        indoc!(
            "
            select * from eth.eth.logs
            where block_number between 10 and 20
            "
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        matches!(
            filter.block_option,
            FilterBlockOption::Range {
                from_block: Some(BlockNumber::Number(U64([10]))),
                to_block: Some(BlockNumber::Number(U64([20]))),
            }
        ),
        "{:?}",
        filter
    );
    assert!(matches!(filter.address, None), "{:?}", filter);
    assert!(
        matches!(filter.topics, [None, None, None, None]),
        "{:?}",
        filter
    );

    // ---
    let filter = datafusion_ethers::convert::sql_to_pushdown_filter(
        &df_ctx,
        indoc!(
            "
            select * from eth.eth.logs
            where
                block_number between 10 and 20
                and topic0 = X'000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266'
                and address = X'5fbdb2315678afecb367f032d93f642f64180aa3'
            "
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        matches!(
            filter.block_option,
            FilterBlockOption::Range {
                from_block: Some(BlockNumber::Number(U64([10]))),
                to_block: Some(BlockNumber::Number(U64([20]))),
            }
        ),
        "{:?}",
        filter
    );
    assert!(
        matches!(
            filter.address,
            Some(ValueOrArray::Value(H160(v)))
            if v == hex!("5fbdb2315678afecb367f032d93f642f64180aa3")
        ),
        "{:?}",
        filter
    );
    assert!(
        matches!(
            filter.topics,
            [Some(ValueOrArray::Value(Some(H256(v)))), None, None, None]
            if v == hex!("000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266")
        ),
        "{:?}",
        filter
    );

    // ---
    let filter = datafusion_ethers::convert::sql_to_pushdown_filter(
        &df_ctx,
        indoc!(
            "
            select
                'foo' as foo,
                block_number
            from eth.eth.logs
            where
                block_number between 10 and 20
                and topic0 = X'000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266'
                and address = X'5fbdb2315678afecb367f032d93f642f64180aa3'
                and log_index = 1
            limit 10
            "
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        matches!(
            filter.block_option,
            FilterBlockOption::Range {
                from_block: Some(BlockNumber::Number(U64([10]))),
                to_block: Some(BlockNumber::Number(U64([20]))),
            }
        ),
        "{:?}",
        filter
    );
    assert!(
        matches!(
            filter.address,
            Some(ValueOrArray::Value(H160(v)))
            if v == hex!("5fbdb2315678afecb367f032d93f642f64180aa3")
        ),
        "{:?}",
        filter
    );
    assert!(
        matches!(
            filter.topics,
            [Some(ValueOrArray::Value(Some(H256(v)))), None, None, None]
            if v == hex!("000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266")
        ),
        "{:?}",
        filter
    );
}

///////////////////////////////////////////////////////////////////////////////////////////////////
