FROM mcr.microsoft.com/dotnet/sdk:8.0
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY clients/csharp /src/clients/csharp
COPY crates/diavasi/proto /src/crates/diavasi/proto
WORKDIR /src/clients/csharp/Diavasi.Data
# The protoc binary shipped in Grpc.Tools segfaults on linux/arm64 (exit 139).
RUN dotnet build -c Release -p:Protobuf_ProtocFullPath=/usr/bin/protoc
COPY clients/docker/wait-ca.sh /wait-ca.sh
RUN chmod +x /wait-ca.sh
CMD ["/bin/sh", "-c", "/wait-ca.sh && dotnet run -c Release --no-build -- --addr \"$DIAVASI_DATA_ADDR\" --ca \"$DIAVASI_CA\" --token \"$DIAVASI_API_TOKEN\" --group demo --consumer csharp --total 8"]
