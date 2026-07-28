# Shared Cargo cache smoke test

This marker exists only to trigger an ordinary pull-request CI run after the
trusted `main` workflow populated the shared Cargo target cache. It does not
change the project or its CI configuration.

The run measures whether a brand-new pull request can restore the Cargo target
artifacts saved for its base commit instead of compiling from a cold target
directory.
