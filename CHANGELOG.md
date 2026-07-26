# Changelog

All notable user-visible changes to this workspace are documented here.

## Unreleased

### Changed

The control-loop rewrite in [#169](https://github.com/ralexstokes/tokio-otp/pull/169)
made these breaking changes to control-operation timing and results:

- A terminal startup failure from one child in an ordered startup sequence no
  longer prevents later siblings from starting.
- A dynamic add reports success once membership is inserted and startup is
  scheduled, even if the supervisor stops immediately afterwards. It no longer
  changes that accepted result to `ControlError::SupervisorStopping`.
- A `remove_child` accepted before shutdown completes with `Ok(())` when
  shutdown absorbs the in-flight removal, rather than returning
  `ControlError::SupervisorStopping`.
- A second `remove_child` for the same id now fails immediately with
  `ControlError::ChildRemovalInProgress`; it no longer queues and later returns
  `ControlError::UnknownChildId`.
