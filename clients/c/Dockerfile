FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends build-essential pkg-config libgrpc-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY clients/c /src
RUN make
COPY clients/docker/wait-ca.sh /wait-ca.sh
RUN chmod +x /wait-ca.sh
CMD ["/bin/sh", "-c", "/wait-ca.sh && /src/diavasi_consume --addr \"$DIAVASI_DATA_ADDR\" --ca \"$DIAVASI_CA\" --token \"$DIAVASI_API_TOKEN\" --group demo --consumer c --total 8"]
