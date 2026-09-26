# Jupyter image

The first build takes a long time. The image installs six kernels, each in its own stage: Python (`ipykernel`), Rust (`evcxr_jupyter`), Go (`gophernotes`), JavaScript (`tslab`), Java (`IJava`), and C# (`dotnet-interactive`).

Elixir is not in this image. The Livebook is [notebooks/demo.livemd](https://github.com/diavasis/diavasi-elixir/blob/main/notebooks/demo.livemd) in `diavasi-elixir`.

C is not in this image. `jupyter-c-kernel` compiles a snippet and exits, so it cannot hold a TLS gRPC stream. The C demo is the Compose profile `c`.

CI does not build this image.

```bash
docker compose -f clients/docker-compose.yml --profile notebook up
```

JupyterLab listens on port 8888. Livebook listens on port 8080. Starter notebooks are in `work/`.
