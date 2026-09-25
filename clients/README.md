# Client SDKs

Thin clients of `diavasi.data.v1`. They connect, authenticate, join, iterate batches, and ack by `batch_id`. None of them compute a checkpoint. The contract is [ADR 0012](../docs/adr/0012-client-sdks.md).

| Language | Tree |
| --- | --- |
| Elixir | `clients/elixir` (`Diavasi.Data.Client`) |
| Rust | `clients/rust` (`diavasi-client`, not a server dependency) |
| Python | `clients/python` (`diavasi_data.consume`) |
| Go | `clients/go` |
| JavaScript | `clients/js` |
| Java | `clients/java` |
| C# | `clients/csharp` |
| C | `clients/c` |

Each tree has a README, one example, and a test that skips until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set.

```bash
./clients/scripts/compat.sh
```

That script starts `diavasi serve` with a synthetic group and runs every SDK that is installed. `DIAVASI_SDK_REQUIRE=1` fails the run when a toolchain is missing. CI uses that flag. `check`, `coverage`, and `deny` do not start this server and do not install these toolchains.

## Compose demos

```bash
docker compose -f clients/docker-compose.yml --profile python up --abort-on-container-exit
```

The same shape works for `elixir`, `rust`, `go`, `js`, `java`, `csharp`, and `c`. `--profile all` starts every demo. The server writes the data-plane CA onto a volume. A synthetic group named `demo` is created before the demo runs. No database container is involved.

## Notebooks

```bash
docker compose -f clients/docker-compose.yml --profile notebook up
```

JupyterLab is on port 8888. Kernels: Python (`ipykernel`), Rust (`evcxr_jupyter`), Go (`gophernotes`), JavaScript (`tslab`), Java (`IJava`), and C# (`dotnet-interactive`). The first image build takes a long time. Elixir is the Livebook service on port 8080 (`clients/elixir/notebooks/demo.livemd`). C has no Jupyter kernel here. The C demo is the Compose profile. CI does not build this image.

The Stage 0 TCP bench clients remain in the Python and Elixir trees. They speak a different protocol from `data.proto`.
