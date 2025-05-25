# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [47.1.0] - 2025-05-25
### Changed
- Upgraded to `alloy v1`

## [47.0.0] - 2025-04-21
### Changed
- Upgraded to `datafusion v47`

## [46.0.0] - 2025-03-16
### Changed
- Upgraded to `datafusion v46`

## [45.0.0] - 2025-02-07
### Changed
- Upgraded to `datafusion v45`

## [44.0.0] - 2025-01-09
### Changed
- Upgraded to latest versions of `datafusion` and `alloy`

## [43.0.0] - 2024-11-08
### Changed
- Upgraded to latest versions of `datafusion` and `alloy`

## [42.0.0] - 2024-09-17
### Changed
- Upgraded to latest versions of `datafusion` and `alloy`

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
