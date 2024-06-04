use datafusion::{
    config::{ConfigEntry, ConfigExtension, ExtensionOptions},
    error::DataFusionError,
};
use ethers::prelude::*;

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct EthProviderConfig {
    pub block_range_from: BlockNumber,
    pub block_range_to: BlockNumber,
}

///////////////////////////////////////////////////////////////////////////////////////////////////

impl Default for EthProviderConfig {
    fn default() -> Self {
        Self {
            block_range_from: BlockNumber::Earliest,
            block_range_to: BlockNumber::Latest,
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
                description:
                    "Lower boundry (inclusive) restriction on block range when pushing down predicate to the node",
            },
            ConfigEntry {
                key: "block_range_to".to_string(),
                value: Some(self.block_range_to.to_string()),
                description:
                    "Upper boundry (inclusive) restriction on block range when pushing down predicate to the node",
            },
        ]
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

fn parse_block_number(s: &str) -> Result<BlockNumber, String> {
    s.to_lowercase().parse::<BlockNumber>()
}
