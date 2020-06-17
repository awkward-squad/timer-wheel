# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to the [Haskell Package Versioning Policy](https://pvp.haskell.org/).

## [0.2.1] 2020-06-16

### Added
- Add support for GHC 8.8, GHC 8.10

### Changed
- Fix underflow bug that affected recurring timers

### Removed
- Remove support for GHC < 8.6

## [0.2.0.1] - 2019-05-19

### Changed
- Swap out `ghc-prim` and `primitive` for `vector`

## [0.2.0] - 2019-02-03

### Added
- Add `destroy` function, for reaping the background thread
- Add `recurring_` function

### Changed
- If the timer wheel reaper thread crashes, it will propagate the exception to
the thread that spawned it
- `new` may now throw `InvalidTimerWheelConfig`
- The cancel action returned by `register` is now memoized, which fixes a bug
involving trying to cancel a `recurring` timer twice. The second call used to
spin forever and peg a CPU
- Use `Config` type for creating a timer wheel
- Change argument order around
- Rename `new` to `create`
- Make recurring timers more accurate

## [0.1.0] - 2018-07-18

### Added
- Initial release
