# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.0.1 - 2024-06-26

### Fixed

- Ignored unknown info messages in `Volley.InOrderSubscription` and
  `Volley.PersistentSubscription`.
    - This protects against the `{:caught_up, reference()}` and
      `{:fell_behind, reference()}` notifications introduced in
      EventStore 23.10 and later.

## 1.0.0 - 2022-05-11

This change represents stability in the API. There is no functional change
from v0.4.1.

## 0.4.1 - 2021-08-30

### Changed

- Allowed Broadway `~> 1.0`
    - The v1.0.0 changes in Broadway do not break any functionality in
      volley

## 0.4.0 - 2021-06-07

### Added

- Added `:subscribe_on_init?` and `:subscribe_after` options to the InOrder
  subscription
    - these can be used to prevent an InOrder producer from producing events
      immediately after starting up

### Changed

- The `:restore_stream_position!` for InOrder subscriptions may now be either
  an MFA tuple or a function (anonymous or capture)

## 0.3.2 - 2021-06-04

### Fixed

- Fixed name determination for InOrder subscriptions (for real this time)

## 0.3.1 - 2021-06-04

### Fixed

- Fixed name determination for InOrder subscriptions

## 0.3.0 - 2021-06-04

### Added

- Added the `:producer` key to each event's `.metadata` map for InOrder
  subscriptions
    - This can inform the consumer of how it should store stream positions

## 0.2.0 - 2021-06-01

### Changed

- Renamed `Volley.LinearSubscription` to `Volley.InOrderSubscription`

### Fixed

- Fixed subscriptions to empty streams in `Volley.InOrderSubscription`.

## 0.1.3 - 2021-05-03

### Changed

- Changed some language in the documentation to remove "head-of-line" blocking
  from the vocabulary

## 0.1.2 - 2021-04-29

### Changed

- Updated Spear dependency to v0.9.0 with fixes for persistent subscription
  ack/nack on projected streams

## 0.1.1 - 2021-04-28

### Changed

- Changed docs in `Volley.LinearSubscription` to suggest a `:max_demand` of `1`
  but not require it

## 0.1.0 - 2021-04-26

### Added

- Initial implementations of persistent and linear subscriptions
