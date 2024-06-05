# Ethereum RPC Bridge for Apache Datafusion

<div align="center">

[![Release](https://img.shields.io/crates/v/datafusion-ethers?include_prereleases&logo=rust&logoColor=orange&style=for-the-badge)](https://crates.io/crates/datafusion-ethers)
[![CI](https://img.shields.io/github/actions/workflow/status/kamu-data/datafusion-ethers/build.yaml?logo=githubactions&label=CI&logoColor=white&style=for-the-badge&branch=master)](https://github.com/kamu-data/datafusion-ethers/actions)
[![Dependencies](https://deps.rs/repo/github/kamu-data/datafusion-ethers/status.svg?&style=for-the-badge)](https://deps.rs/repo/github/kamu-data/datafusion-ethers)
[![Chat](https://shields.io/discord/898726370199359498?style=for-the-badge&logo=discord&label=Discord)](https://discord.gg/nU6TXRQNXC)

</div>

## About
This adapter allows querying data from Ethereum-compatible blockchain nodes using SQL.

SQL queries are analyzed and optimized by [Apache Datafusion](https://github.com/apache/arrow-datafusion) engine and custom physical plan nodes translate them into [Ethereum JSON-RPC](https://ethereum.org/en/developers/docs/apis/json-rpc/) calls to the node, maximally utilizing the predicate push-down.

The crate also provides UDFs (a set of custom SQL functions) for ABI decoding.

The crate is currently designed for embedding, but the goal is also to provide an application that supports [FlightSQL](https://arrow.apache.org/docs/format/FlightSql.html) protocol and streaming data queries.

While the crate currently relies on semi-deprecated [`ethers` crate](https://github.com/gakonst/ethers-rs) we will be moving it progressively to [`alloy`](https://github.com/alloy-rs/alloy) as it matures - any help with that is much appreciated.


## Quick Start
```rust
// Create `ethers` RPC client
let rpc_client = Arc::new(Provider::<Http>::connect("http://localhost:8545").await);

// Add config extension with RPC endpoint URL
let mut cfg = SessionConfig::new()
    .with_option_extension(datafusion_ethers::config::EthProviderConfig::default());

// Create `datafusion` session
let mut ctx = SessionContext::new_with_config(cfg);

// Register all UDFs
datafusion_ethers::udf::register_all(&mut ctx).unwrap();

// (Optional) Register JSON UDFs
datafusion_functions_json::register_all(&mut ctx).unwrap();

// Register catalog provider
ctx.register_catalog(
    "eth",
    Arc::new(datafusion_ethers::provider::EthCatalog::new(rpc_client)),
);

let df = ctx.sql("...").await.unwrap();
df.show().await.unwrap();
```

## Example Queries
Get raw logs:
```sql
select *
from eth.eth.logs
where block_number between 0 and 5
limit 5
```
```sh
+--------------+------------------------------------------------------------------+-------------------+------------------------------------------------------------------+-----------+------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| block_number | block_hash                                                       | transaction_index | transaction_hash                                                 | log_index | address                                  | topic0                                                           | topic1                                                           | topic2                                                           | topic3 | data                                                                                                                                                                                             |
+--------------+------------------------------------------------------------------+-------------------+------------------------------------------------------------------+-----------+------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 3            | 0cbb8ed5d37f3573b74bc29657c5a847decbb6305d5e4daf26e5f8aa5e64b9e1 | 0                 | ddba13f2509c99ce7f194cf77d754b4134255e24c1b104eddc4cb690c5582379 | 0         | 5fbdb2315678afecb367f032d93f642f64180aa3 | d9e93ef3ac030ca8925f1725575c96d8a49bd825c0843a168225c1bb686bba67 | 000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266 | 000000000000000000000000000000000000000000000000000000000000007b |        |                                                                                                                                                                                                  |
| 3            | 0cbb8ed5d37f3573b74bc29657c5a847decbb6305d5e4daf26e5f8aa5e64b9e1 | 0                 | ddba13f2509c99ce7f194cf77d754b4134255e24c1b104eddc4cb690c5582379 | 1         | 5fbdb2315678afecb367f032d93f642f64180aa3 | da343a831f3915a0c465305afdd6b0f1c8a3c85635bb14272bf16b6de3664a51 | 0000000000000000000000005fbdb2315678afecb367f032d93f642f64180aa3 |                                                                  |        | 00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000005612d626172000000000000000000000000000000000000000000000000000000 |
| 4            | 602be5f1d8b8b3f408accb42875220ab6c5b6e39ba82a91c531bb8bc9fef0954 | 0                 | 554478d501eee16dcd25f6bd30be3a2251daf9a02e643d152fcfc59934a87fbd | 0         | e7f1725e7734ce288f8367e1bb143e90bb3f0512 | d9e93ef3ac030ca8925f1725575c96d8a49bd825c0843a168225c1bb686bba67 | 000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266 | 000000000000000000000000000000000000000000000000000000000000007b |        |                                                                                                                                                                                                  |
| 4            | 602be5f1d8b8b3f408accb42875220ab6c5b6e39ba82a91c531bb8bc9fef0954 | 0                 | 554478d501eee16dcd25f6bd30be3a2251daf9a02e643d152fcfc59934a87fbd | 1         | e7f1725e7734ce288f8367e1bb143e90bb3f0512 | da343a831f3915a0c465305afdd6b0f1c8a3c85635bb14272bf16b6de3664a51 | 000000000000000000000000e7f1725e7734ce288f8367e1bb143e90bb3f0512 |                                                                  |        | 00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000005612d626172000000000000000000000000000000000000000000000000000000 |
+--------------+------------------------------------------------------------------+-------------------+------------------------------------------------------------------+-----------+------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

Get raw logs from a specific contract address:
```sql
select *
from eth.eth.logs
where address = X'5fbdb2315678afecb367f032d93f642f64180aa3'
```

Get raw logs matching specific signature:
```sql
select *
from eth.eth.logs
where topic0 = eth_event_selector('MyLog(address indexed addr, uint64 indexed id)')
```

Decode raw log data into JSON:
```sql
select
  eth_decode_event(
    'SendRequest(uint64 indexed requestId, address indexed consumerAddr, bytes request)',
    topic0,
    topic1,
    topic2,
    topic3,
    data
  ) as event
from eth.eth.logs
```
```sh
+-----------------------------------------------------------------------------------------------------------------------+
| event                                                                                                                 |
+-----------------------------------------------------------------------------------------------------------------------+
| {"consumerAddr":"aabbccddaabbccddaabbccddaabbccddaabbccdd","name":"SendRequest","request":"ff00bbaa","requestId":123} |
+-----------------------------------------------------------------------------------------------------------------------+
```

Extract decoded event into a nicely typed table (using [`datafusion-functions-json`](https://github.com/datafusion-contrib/datafusion-functions-json) crate):
```sql
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
  from eth.eth.logs
)
```
```sh
+-------------+------------+------------------------------------------+----------+
| name        | request_id | consumer_addr                            | request  |
+-------------+------------+------------------------------------------+----------+
| SendRequest | 123        | aabbccddaabbccddaabbccddaabbccddaabbccdd | ff00bbaa |
+-------------+------------+------------------------------------------+----------+
```

> Note: Currently DataFusion does not allow UDFs to produce different resulting data types depending on the arguments. Therefore we cannot analyze the event signature literal and return a corresponding nested struct from the UDF. Therefore we have to use JSON as a return value. If you would like to skip JSON and go directly to the nested struct take a look at `EthDecodedLogsToArrow` and `EthRawAndDecodedLogsToArrow` transcoders.

## Supported Features
- Table: `logs`
  - Can be used to access transaction log events, with schema corresponding closely to [`eth_getLogs`](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_getlogs) RPC method
  - Supports predicate push-down on `block_number`, `block_hash`, `address`, `topic[0-3]`
  - Supports `limit`
- Config options:
  - `block_range_from` (default `earliest`) - lower boundry (inclusive) restriction on block range when pushing down predicate to the node
  - `block_range_to` (default `latest`)  - upper boundry (inclusive) restriction on block range when pushing down predicate to the node",
- UDF: `eth_event_selector(<solidity log signature>): hash`
  - Converts Solidity log signature into a hash
  - Useful for filtering events by `topic0`
- UDF: `eth_(try_)decode_event(<solidity log signature>), topic0, topic1, topic2, topic3, data): json string`
  - Decodes raw event data into JSON string
  - JSON can then be further processed using [`datafusion-functions-json`](https://github.com/datafusion-contrib/datafusion-functions-json) crate
- Transcoders:
  - `EthRawLogsToArrow` - converts raw log data into Arrow record batches
  - `EthDecodedLogsToArrow` - converts decoded log data into Arrow record batches
  - `EthRawAndDecodedLogsToArrow` - given raw log data and an event type and produces Arrow record batches with raw data columns and with decoded event columns as a nested struct
- Utilities:
  - `RawLogsStream` - implements efficient and resumable pagination of `eth_getLogs` using a provided filter

## Related Projects
- [`kamu-cli`](https://github.com/kamu-data/kamu-cli) - verifiable data processing toolset that can ingest data from blockchains and provide it to smart contracts as a new-generation oracle.
