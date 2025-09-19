use std::sync::Arc;

use alloy::providers::{Provider, ProviderBuilder};
use datafusion::prelude::*;

#[tokio::main]
async fn main() {
    init_tracing();

    let url = "https://0xrpc.io/eth";

    tracing::info!(url, "Fetching data from Ethereum Mainnet");

    let rpc_client = ProviderBuilder::new_with_network::<alloy::network::any::AnyNetwork>()
        .connect(url)
        .await
        .unwrap()
        .erased();

    let config = datafusion_ethers::config::EthProviderConfig {
        block_stride: 10,
        ..Default::default()
    };

    let mut ctx =
        SessionContext::new_with_config(SessionConfig::new().with_option_extension(config.clone()));

    datafusion_ethers::udf::register_all(&mut ctx).unwrap();

    ctx.register_catalog(
        "eth",
        Arc::new(datafusion_ethers::provider::EthCatalog::new(
            config, rpc_client,
        )),
    );

    let df = ctx
        .sql(
            r#"
            select
                block_number,
                block_timestamp,
                eth_decode_event(
                    'TokensMinted(address indexed to, uint256 amount, uint256 ethAmount, uint256 time)',
                    topic0,
                    topic1,
                    topic2,
                    topic3,
                    data
                ) as event
            from eth.eth.logs
            where
                block_number > 13337653
                and topic0 = eth_event_selector(
                    'TokensMinted(address indexed to, uint256 amount, uint256 ethAmount, uint256 time)'
                )
            limit 1
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
