PERF-001 `passed` — record_only_under_wsl

| surface | disposition | startup (ms) | steady median (ms) | max abs delta | predictor |
|---|---|---:|---:|---:|---|
| python_oracle | passed | 1067.573 | 28.026 | 0 | c130231adf74 |
| rust_direct | passed | 38.148 | 18.244 | 0 | c130231adf74 |
| studio_rust | passed | 75.621 | 24.771 | 0 | c130231adf74 |
| web_wasm | passed | 97.810 | 4.713 | 0 | c130231adf74 |

Startup and steady-state are separate. No definitive performance threshold is applied.
