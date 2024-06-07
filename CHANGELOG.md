# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [38.2.0] - 2024-06-07
### Changed
- Fully migrated from deprecated `ethers` to `alloy` crate

## [38.1.1] - 2024-06-05
### Changed
- Minor linter fixes

## [38.1.0] - 2024-06-05
### Added
- `RawLogsStream` resumable filtered log stream
- New transcoder types:
  - `EthRawLogsToArrow` - converts raw log data into Arrow record batches
  - `EthDecodedLogsToArrow` - converts decoded log data into Arrow record batches
  - `EthRawAndDecodedLogsToArrow` - given raw log data and an event type and produces Arrow record batches with raw data columns and with decoded event columns 
- Fallible `eth_try_decode_event` UDF

## [38.0.0] - 2024-05-28
### Added
- Initial release
