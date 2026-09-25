FROM golang:1.23-bookworm
WORKDIR /src
COPY clients/go /src
RUN go mod download && go build -o /consume ./cmd/consume
COPY clients/docker/wait-ca.sh /wait-ca.sh
RUN chmod +x /wait-ca.sh
CMD ["/bin/sh", "-c", "/wait-ca.sh && /consume --addr \"$DIAVASI_DATA_ADDR\" --ca \"$DIAVASI_CA\" --token \"$DIAVASI_API_TOKEN\" --group demo --consumer go --total 8"]
