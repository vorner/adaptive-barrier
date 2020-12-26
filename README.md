# Adaptive barrier

[![Actions Status](https://github.com/vorner/adaptive-barrier/workflows/test/badge.svg)](https://github.com/vorner/adaptive-barrier/actions)
[![codecov](https://codecov.io/gh/vorner/adaptive-barrier/branch/main/graph/badge.svg?token=0FhwzST2nI)](https://codecov.io/gh/vorner/adaptive-barrier)
[![docs](https://docs.rs/adaptive-barrier/badge.svg)](https://docs.rs/adaptive-barrier)

A Barrier synchronization primitive, similar to `std::sync::Barrier`, but one
that adjusts the expected number of threads. This makes it robust in face of
panics (it won't make your program deadlock, like the standard `Barrier`).

## License

Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms
or conditions.
