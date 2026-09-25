# The first build takes a long time. Each kernel is its own stage.
# Elixir is the Livebook service. C has no kernel in this image.

FROM rust:1.95-bookworm AS rust-kernel
RUN cargo install evcxr_jupyter --version 0.22.0 --locked
RUN evcxr_jupyter --install

FROM golang:1.23-bookworm AS go-kernel
RUN go install github.com/gopherdata/gophernotes@v0.7.5
RUN mkdir -p /kernels/gophernotes \
    && dir=$(go env GOPATH)/pkg/mod/github.com/gopherdata/gophernotes@v0.7.5 \
    && cp "$dir/kernel/"* /kernels/gophernotes/ \
    && sed -i "s|gophernotes|$(go env GOPATH)/bin/gophernotes|" /kernels/gophernotes/kernel.json

FROM node:22-bookworm AS js-kernel
RUN npm install -g tslab \
    && mkdir -p /usr/local/share/jupyter/kernels/jslab /usr/local/share/jupyter/kernels/tslab \
    && printf '%s\n' '{"argv":["/usr/local/bin/tslab","kernel","--config-path","{connection_file}","--js"],"display_name":"JavaScript","language":"javascript"}' > /usr/local/share/jupyter/kernels/jslab/kernel.json \
    && printf '%s\n' '{"argv":["/usr/local/bin/tslab","kernel","--config-path","{connection_file}"],"display_name":"TypeScript","language":"typescript"}' > /usr/local/share/jupyter/kernels/tslab/kernel.json

FROM eclipse-temurin:21-jdk-jammy AS java-kernel
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-jupyter-client unzip curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL -o /tmp/ijava.zip https://github.com/SpencerPark/IJava/releases/download/v1.3.0/ijava-1.3.0.zip \
    && mkdir -p /tmp/ijava && unzip /tmp/ijava.zip -d /tmp/ijava \
    && python3 /tmp/ijava/install.py --prefix /usr/local

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS cs-kernel
RUN dotnet tool install -g Microsoft.dotnet-interactive --version 1.0.556801
ENV PATH="/root/.dotnet/tools:${PATH}"
ENV DOTNET_INTERACTIVE_CLI_TELEMETRY_OPTOUT=1
RUN mkdir -p /root/.local/share/jupyter/kernels \
    && dotnet interactive jupyter install

FROM python:3.12-bookworm
RUN pip install --no-cache-dir jupyterlab ipykernel grpcio protobuf
COPY --from=rust-kernel /usr/local/cargo /usr/local/cargo
COPY --from=rust-kernel /usr/local/rustup /usr/local/rustup
COPY --from=rust-kernel /root/.local/share/jupyter /usr/local/share/jupyter
COPY --from=go-kernel /go/bin/gophernotes /usr/local/bin/gophernotes
COPY --from=go-kernel /kernels/gophernotes /usr/local/share/jupyter/kernels/gophernotes
RUN sed -i 's|/go/bin/gophernotes|/usr/local/bin/gophernotes|' /usr/local/share/jupyter/kernels/gophernotes/kernel.json
COPY --from=js-kernel /usr/local/bin/node /usr/local/bin/node
COPY --from=js-kernel /usr/local/bin/tslab /usr/local/bin/tslab
COPY --from=js-kernel /usr/local/lib/node_modules /usr/local/lib/node_modules
COPY --from=js-kernel /usr/local/share/jupyter /usr/local/share/jupyter
COPY --from=java-kernel /opt/java /opt/java
COPY --from=java-kernel /usr/local/share/jupyter /usr/local/share/jupyter
COPY --from=cs-kernel /root/.dotnet /root/.dotnet
COPY --from=cs-kernel /root/.local/share/jupyter /usr/local/share/jupyter
ENV PATH="/usr/local/cargo/bin:/opt/java/openjdk/bin:/root/.dotnet/tools:${PATH}"
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV JAVA_HOME=/opt/java/openjdk
ENV DOTNET_ROOT=/root/.dotnet
COPY clients/notebook/work /work
WORKDIR /work
EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--ServerApp.token=", "--ServerApp.password=", "--notebook-dir=/work"]
