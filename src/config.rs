use alloy::rpc::types::eth::{BlockNumberOrTag, Filter};
use datafusion::{
    config::{ConfigEntry, ConfigExtension, ExtensionOptions},
    error::DataFusionError,
};

use crate::stream::StreamOptions;

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct EthProviderConfig {
    pub block_range_from: BlockNumberOrTag,
    pub block_range_to: BlockNumberOrTag,
    pub block_stride: u64,
}

///////////////////////////////////////////////////////////////////////////////////////////////////

impl Default for EthProviderConfig {
    fn default() -> Self {
        Self {
            block_range_from: BlockNumberOrTag::Earliest,
            block_range_to: BlockNumberOrTag::Latest,
            block_stride: 100_000,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

impl EthProviderConfig {
    pub fn default_filter(&self) -> Filter {
        Filter::new()
            .from_block(self.block_range_from)
            .to_block(self.block_range_to)
    }

    pub fn stream_options(&self) -> StreamOptions {
        StreamOptions {
            block_stride: self.block_stride,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

impl ConfigExtension for EthProviderConfig {
    const PREFIX: &'static str = "ethereum";
}

///////////////////////////////////////////////////////////////////////////////////////////////////

impl ExtensionOptions for EthProviderConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        &mut *self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> datafusion::error::Result<()> {
        match key {
            "block_range_from" => {
                self.block_range_from = parse_block_number(value)
                    .map_err(|msg| DataFusionError::Configuration(msg.to_string()))?;
                Ok(())
            }
            "block_range_to" => {
                self.block_range_to = parse_block_number(value)
                    .map_err(|msg| DataFusionError::Configuration(msg.to_string()))?;
                Ok(())
            }
            _ => Err(DataFusionError::Configuration(format!(
                "Unsupported option: {key}"
            ))),
        }
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![
            ConfigEntry {
                key: "block_range_from".to_string(),
                value: Some(self.block_range_from.to_string()),
                description: "Lower boundry (inclusive) restriction on block range when pushing down predicate to the node",
            },
            ConfigEntry {
                key: "block_range_to".to_string(),
                value: Some(self.block_range_to.to_string()),
                description: "Upper boundry (inclusive) restriction on block range when pushing down predicate to the node",
            },
        ]
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

fn parse_block_number(s: &str) -> Result<BlockNumberOrTag, String> {
    let block = match s.to_lowercase().as_str() {
        "latest" => BlockNumberOrTag::Latest,
        "finalized" => BlockNumberOrTag::Finalized,
        "safe" => BlockNumberOrTag::Safe,
        "earliest" => BlockNumberOrTag::Earliest,
        "pending" => BlockNumberOrTag::Pending,
        number => BlockNumberOrTag::Number(
            number
                .parse()
                .map_err(|_| format!("Invalid block number: {number}"))?,
        ),
    };
    Ok(block)
}
