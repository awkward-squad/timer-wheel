# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to the [Haskell Package Versioning Policy](https://pvp.haskell.org/).

## Unreleased

### Changed
- If the timer wheel reaper thread crashes, it will propagate the exception to
the thread that spawned it.
- `new` may now throw `InvalidTimerWheelConfig`

## [0.1.0] - 2018-07-18

### Added
- Initial release
