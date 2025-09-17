use std::sync::Arc;

use alloy::providers::{Provider, ProviderBuilder};
use datafusion::prelude::*;

#[tokio::main]
async fn main() {
    init_tracing();

    let url = "https://quicknode1.peaq.xyz";

    tracing::info!(url, "Fetching data from PEAQ network");

    let rpc_client = ProviderBuilder::new_with_network::<alloy::network::any::AnyNetwork>()
        .connect(url)
        .await
        .unwrap()
        .erased();

    let config = datafusion_ethers::config::EthProviderConfig {
        schema_name: "peaq".into(),
        block_stride: 1_000,
        // NOTE: Peaq does not include block timestamps in `eth_getLogs` response
        use_block_timestamp_fallback: true,
        ..Default::default()
    };

    let mut ctx =
        SessionContext::new_with_config(SessionConfig::new().with_option_extension(config.clone()));

    datafusion_ethers::udf::register_all(&mut ctx).unwrap();

    datafusion_functions_json::register_all(&mut ctx).unwrap();

    ctx.register_catalog(
        "peaq",
        Arc::new(datafusion_ethers::provider::EthCatalog::new(
            config, rpc_client,
        )),
    );

    let df = ctx
        .sql(
            r#"
            with decoded as (
                select
                    block_number,
                    block_timestamp,
                    eth_decode_event(
                        'AddAttribute(address sender, address did_account, bytes name, bytes value, uint32 validity)',
                        topic0,
                        topic1,
                        topic2,
                        topic3,
                        data
                    ) as event
                from peaq.peaq.logs
                where
                    block_number > 3717155
                    and topic0 = eth_event_selector(
                        'AddAttribute(address sender, address did_account, bytes name, bytes value, uint32 validity)'
                    )
            )
            select
                block_number,
                block_timestamp,
                cast(
                    decode(
                        json_get_str(event, 'name'),
                        'hex'
                    )
                    as String
                ) as name
            from decoded
            limit 3
            "#,
        )
        .await
        .unwrap();

    println!("{:#?}", df.schema());
    df.show().await.unwrap();
}

fn init_tracing() {
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::util::SubscriberInitExt as _;
    use tracing_subscriber::*;

    let text_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_writer(std::io::stderr)
        .with_line_number(true)
        .with_thread_names(true)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE);

    tracing_subscriber::registry()
        .with(
            EnvFilter::builder()
                .with_default_directive(filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with(text_layer)
        .init();
}
