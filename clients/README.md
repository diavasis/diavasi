# Client tools

Thin clients of `diavasi.data.v1`. They connect with TLS, send a bearer token, join a group, yield batches, and ack by `batch_id`. The server owns the cursor. The contract is [ADR 0012](../docs/adr/0012-client-sdks.md).

Packages are unpublished. Install from this repo. The name in the last column is the name reserved for a later publish.

| Language | Guide | Install today | Reserved name | Compose profile |
| --- | --- | --- | --- | --- |
| Elixir | [README](elixir/README.md) | Mix path `clients/elixir` | `diavasi` on Hex | `elixir` |
| Rust | [README](rust/README.md) | path dependency `clients/rust` | `diavasi-client` on crates.io | `rust` |
| Python | [README](python/README.md) | `PYTHONPATH=clients/python` | `diavasi-data` on PyPI | `python` |
| Go | [README](go/README.md) | module `github.com/diavasis/diavasi/clients/go` | that module path | `go` |
| JavaScript | [README](js/README.md) | `npm install` in `clients/js` | `@diavasi/data` on npm | `js` |
| Java | [README](java/README.md) | Gradle project `clients/java` | Maven Central, unpublished | `java` |
| C# | [README](csharp/README.md) | `clients/csharp/Diavasi.Data` | NuGet, unpublished | `csharp` |
| C | [README](c/README.md) | `make` in `clients/c` | none | `c` |
| Zig | [README](zig/README.md) | `zig build` in `clients/zig` | none | none |

Each guide has the library call, the example flags, protocol errors 1 through 8, and a test that skips until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set. The Elixir guide also shows a GenServer consumer, `Task.async/1`, Flow, GenStage, and Broadway.

```bash
./clients/scripts/compat.sh
```

That script starts `diavasi serve` with a synthetic group and runs every SDK that is installed. `DIAVASI_SDK_REQUIRE=1` fails the run when a toolchain is missing. CI uses that flag.

## Compose demos

```bash
docker compose -f clients/docker-compose.yml --profile python up --abort-on-container-exit
```

The same shape works for `elixir`, `rust`, `go`, `js`, `java`, `csharp`, and `c`. `--profile all` starts every demo. The server writes the data-plane CA onto a volume and creates the synthetic group `demo`. No database container is involved.

## Notebooks

```bash
docker compose -f clients/docker-compose.yml --profile notebook up
```

JupyterLab is on port 8888. Kernels: Python (`ipykernel`), Rust (`evcxr_jupyter`), Go (`gophernotes`), JavaScript and TypeScript (`tslab`), Java (`IJava`), and C# (`dotnet-interactive`). The first image build takes a long time. Elixir is the Livebook service on port 8080, using [elixir/notebooks/demo.livemd](elixir/notebooks/demo.livemd). C and Zig have no Jupyter kernel. The C image build is in the root README under Developing Diavasi. CI does not build this image.

The Stage 0 TCP bench clients remain in the Python and Elixir trees. They speak a different protocol from `data.proto`.
