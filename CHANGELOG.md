## Unreleased

- Add `count`, which returns the number of timers in a timer wheel
- Add `Seconds` type alias for readability
- Make `create` / `with` no longer throw an exception if given an invalid config; rather, the config's invalid values are
  replaced with sensible defaults
- Make `recurring` / `recurring_` handle delays that are shorter than the wheel resolution more correctly
- Make `recurring` / `recurring_` no longer throw an exception if given a negative delay
- Replace `array` with `primitive`
- Make calling `cancel` more than once on a recurring timer not enter an infinite loop
- Make timers that expire in the same batch no longer fire in an arbitrary order
- Simplify and optimize internals

## [0.4.0.1] - 2022-11-05

- Fix inaccurate haddock on `recurring`

## [0.4.0] - 2022-11-05

- Add `create`
- Rename `Data.TimerWheel` to `TimerWheel`
- Swap out `vector` for `array`
- Treat negative delays as 0
- Drop support for GHC < 8.6

## [0.3.0] - 2020-06-18

- Add `with`
- Add support for GHC 8.8, GHC 8.10
- Change type of `spokes` from `Natural` to `Int`
- Change order of delay and action arguments in `register`, `register_`, `recurring`, and `recurring_`
- Simplify `cancel` to return `True` at most once
- Throw an error if a negative delay is provided to `register`, `register_`, `recurring`, or `recurring_`
- Fix underflow bug that affected recurring timers
- Remove `create`, `destroy`
- Remove `TimerWheelDied` exception. `with` now simply re-throws the exception that the timer wheel thread throws
- Remove `InvalidTimerWheelConfig` exception. `error` is used instead
- Remove support for GHC < 8.6

## [0.2.0.1] - 2019-05-19

- Swap out `ghc-prim` and `primitive` for `vector`

## [0.2.0] - 2019-02-03

- Add `destroy` function, for reaping the background thread
- Add `recurring_` function
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

- Initial release
