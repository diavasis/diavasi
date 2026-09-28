# Client tools

Thin clients of `diavasi.data.v1`. They connect with TLS, send a bearer token, join a group, yield batches, and ack by `batch_id`. The server owns the cursor. The contract is [ADR 0012](../docs/adr/0012-client-sdks.md).

Each client is its own repository at version 0.1.0. That is the first publish of the libraries. `proto/data.proto` in those repositories is the copy of [crates/diavasi-core/proto/data.proto](../crates/diavasi-core/proto/data.proto) from server tag `v0.12.0`. Push those repositories before Compose or `clients/scripts/compat.sh` can clone them.

| Language | Repository | Install 0.1.0 | Compose profile |
| --- | --- | --- | --- |
| Elixir | [diavasi-elixir](https://github.com/diavasis/diavasi-elixir) | Hex `{:diavasi, "~> 0.1.0"}` | `elixir` |
| Rust | [diavasi-client](https://github.com/diavasis/diavasi-client) | crates.io `diavasi-client = "0.1.0"` | `rust` |
| Python | [diavasi-python](https://github.com/diavasis/diavasi-python) | PyPI `pip install diavasi-data==0.1.0` | `python` |
| Go | [diavasi-go](https://github.com/diavasis/diavasi-go) | `go get github.com/diavasis/diavasi-go@v0.1.0` | `go` |
| JavaScript | [diavasi-js](https://github.com/diavasis/diavasi-js) | npm `npm install @diavasi/data@0.1.0` | `js` |
| Java | [diavasi-java](https://github.com/diavasis/diavasi-java) | Maven `dev.diavasi:diavasi-data:0.1.0` | `java` |
| C# | [diavasi-dotnet](https://github.com/diavasis/diavasi-dotnet) | NuGet `Diavasi.Data` 0.1.0 | `csharp` |
| C | [diavasi-c](https://github.com/diavasis/diavasi-c) | git tag `v0.1.0` | `c` |
| Zig | [diavasi-zig](https://github.com/diavasis/diavasi-zig) | git tag `v0.1.0`, submodule of `diavasi-c` | none |

The Elixir guide shows a GenServer consumer, `Task.async/1`, Flow, GenStage, and Broadway. The Livebook is [notebooks/demo.livemd](https://github.com/diavasis/diavasi-elixir/blob/main/notebooks/demo.livemd) in that repository.

```bash
./clients/scripts/compat.sh
```

That script starts `diavasi serve` with a synthetic group, clones the client repositories (tag `v0.1.0`, or the default branch until that tag exists), and runs every SDK that is installed. `DIAVASI_SDK_REQUIRE=1` fails the run when a toolchain is missing. CI uses that flag. `DIAVASI_SDK_ROOT` points the script at checkouts you already have.

## Compose demos

```bash
docker compose -f clients/docker-compose.yml --profile python up --abort-on-container-exit
```

The same shape works for `elixir`, `rust`, `go`, `js`, `java`, `csharp`, and `c`. Client images build from the GitHub repositories. `--profile all` starts every demo. The server writes the data-plane CA onto a volume and creates the synthetic group `demo`. No database container is involved.

The published server image is separate from that Compose file and from the database lab in the root `docker-compose.yml`. On a release tag the workflow pushes `ghcr.io/diavasis/diavasi:<version>` and `:latest`, built from the linux-x86_64 release binary.

```bash
docker run --rm -p 7700:7700 -p 7710:7710 \
  -e DIAVASI_API_TOKEN=secret \
  ghcr.io/diavasis/diavasi:0.13.0
```

## Notebooks

```bash
docker compose -f clients/docker-compose.yml --profile notebook up
```

JupyterLab is on port 8888. Kernels: Python (`ipykernel`), Rust (`evcxr_jupyter`), Go (`gophernotes`), JavaScript and TypeScript (`tslab`), Java (`IJava`), and C# (`dotnet-interactive`). The first image build takes a long time. C and Zig have no Jupyter kernel. Elixir is the Livebook in `diavasi-elixir`. CI does not build this image.

The Stage 0 TCP bench clients remain in the Python and Elixir repositories. They speak a different protocol from `data.proto`.
