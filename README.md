# Ethereum RPC Bridge for Apache Datafusion

<div align="center">

[![Release](https://img.shields.io/crates/v/datafusion-ethers?include_prereleases&logo=rust&logoColor=orange&style=for-the-badge)](https://crates.io/crates/datafusion-ethers)
[![CI](https://img.shields.io/github/actions/workflow/status/kamu-data/datafusion-ethers/build.yaml?logo=githubactions&label=CI&logoColor=white&style=for-the-badge&branch=master)](https://github.com/kamu-data/datafusion-ethers/actions)
[![Dependencies](https://deps.rs/repo/github/kamu-data/datafusion-ethers/status.svg?&style=for-the-badge)](https://deps.rs/repo/github/kamu-data/datafusion-ethers)
[![Chat](https://shields.io/discord/898726370199359498?style=for-the-badge&logo=discord&label=Discord)](https://discord.gg/nU6TXRQNXC)

</div>

## About
This adapter allows querying data from Ethereum-compatible blockchain nodes using SQL.

SQL queries are analyzed and optimized by [Apache Datafusion](https://github.com/apache/arrow-datafusion) engine and custom physical plan nodes translate them into [Ethereum JSON-RPC](https://ethereum.org/en/developers/docs/apis/json-rpc/) calls to the node, maximally utilizing the predicate push-down. Node communications are done via [`alloy`](https://github.com/alloy-rs/alloy) crate.

The crate also provides UDFs (a set of custom SQL functions) for ABI decoding.

The crate is currently designed for embedding, but the goal is also to provide an application that supports [FlightSQL](https://arrow.apache.org/docs/format/FlightSql.html) protocol and streaming data queries.


## Quick Start
Setup dependencies:
```toml
# Note: Alloy is still under active development and needs to be used via git
alloy = { git = "https://github.com/alloy-rs/alloy", branch = "main", features = [
    "provider-http",
    "provider-ws",
] }
datafusion = { version = "*" }
```

Initialize libraries and run queries:
```rust
// Create `alloy` RPC client
let rpc_client = ProviderBuilder::new()
    .on_builtin("http://localhost:8545")
    .await
    .unwrap();

// (Optional) Add config extension
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

let df = ctx.sql("select * from eth.eth.logs limit 5").await.unwrap();
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
+--------------+------------------------------------------------------------------+----------------------+-------------------+------------------------------------------------------------------+-----------+------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| block_number | block_hash                                                       | block_timestamp      | transaction_index | transaction_hash                                                 | log_index | address                                  | topic0                                                           | topic1                                                           | topic2                                                           | topic3 | data                                                                                                                                                                                             |
+--------------+------------------------------------------------------------------+----------------------+-------------------+------------------------------------------------------------------+-----------+------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 3            | e5a51ad7ea21386e46be1a9f94ff96be0b5b9d4fd79a898a3aaa759d1dff6ae4 | 2024-06-07T08:14:44Z | 0                 | 944d0ecfa3e3d226b5af093570ba50d743313c0485f236a1414d4781777b5b00 | 0         | 5fbdb2315678afecb367f032d93f642f64180aa3 | d9e93ef3ac030ca8925f1725575c96d8a49bd825c0843a168225c1bb686bba67 | 000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266 | 000000000000000000000000000000000000000000000000000000000000007b |        |                                                                                                                                                                                                  |
| 3            | e5a51ad7ea21386e46be1a9f94ff96be0b5b9d4fd79a898a3aaa759d1dff6ae4 | 2024-06-07T08:14:44Z | 0                 | 944d0ecfa3e3d226b5af093570ba50d743313c0485f236a1414d4781777b5b00 | 1         | 5fbdb2315678afecb367f032d93f642f64180aa3 | da343a831f3915a0c465305afdd6b0f1c8a3c85635bb14272bf16b6de3664a51 | 0000000000000000000000005fbdb2315678afecb367f032d93f642f64180aa3 |                                                                  |        | 00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000005612d626172000000000000000000000000000000000000000000000000000000 |
| 4            | f379253db8ae7ce55c559fc8603b399caa163f95b7e4ef785f3fef50762cc9f2 | 2024-06-07T08:14:45Z | 0                 | 4da3936c231342e2855bc879c4c3a77724142c249bc15065e0c2fc0af28e8072 | 0         | e7f1725e7734ce288f8367e1bb143e90bb3f0512 | d9e93ef3ac030ca8925f1725575c96d8a49bd825c0843a168225c1bb686bba67 | 000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266 | 000000000000000000000000000000000000000000000000000000000000007b |        |                                                                                                                                                                                                  |
| 4            | f379253db8ae7ce55c559fc8603b399caa163f95b7e4ef785f3fef50762cc9f2 | 2024-06-07T08:14:45Z | 0                 | 4da3936c231342e2855bc879c4c3a77724142c249bc15065e0c2fc0af28e8072 | 1         | e7f1725e7734ce288f8367e1bb143e90bb3f0512 | da343a831f3915a0c465305afdd6b0f1c8a3c85635bb14272bf16b6de3664a51 | 000000000000000000000000e7f1725e7734ce288f8367e1bb143e90bb3f0512 |                                                                  |        | 00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000005612d626172000000000000000000000000000000000000000000000000000000 |
+--------------+------------------------------------------------------------------+----------------------+-------------------+------------------------------------------------------------------+-----------+------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

> Note: the `block_time` column will only be populated if the node implementation supports it see [ethereum/execution-apis#295](https://github.com/ethereum/execution-apis/issues/295).

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
