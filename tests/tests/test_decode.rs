use alloy_core::hex;
use alloy_core::sol_types::SolEvent;
use datafusion::prelude::*;
use ethers::prelude::*;
use indoc::indoc;

////////////////////////////////////////////////////////////////////////////////////////

alloy_core::sol! {
    event SendRequest(uint64 indexed requestId, address indexed consumerAddr, bytes request);
}

////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_logs_to_record_batch() {
    let batch = datafusion_ethers::convert::logs_to_record_batch(vec![get_sample_log()]);

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

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_udf_eth_decode_event() {
    let mut ctx = SessionContext::new();
    datafusion_functions_json::register_all(&mut ctx).unwrap();
    datafusion_ethers::udf::register_all(&mut ctx).unwrap();

    ctx.register_batch(
        "logs",
        datafusion_ethers::convert::logs_to_record_batch(vec![get_sample_log()]),
    )
    .unwrap();

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

////////////////////////////////////////////////////////////////////////////////////////

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
