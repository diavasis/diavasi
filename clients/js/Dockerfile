FROM node:22-bookworm-slim
WORKDIR /app
COPY clients/js /app
COPY crates/diavasi/proto/data.proto /proto/data.proto
RUN npm install --omit=dev
COPY clients/docker/wait-ca.sh /wait-ca.sh
RUN chmod +x /wait-ca.sh
ENV DIAVASI_PROTO=/proto/data.proto
CMD ["/bin/sh", "-c", "/wait-ca.sh && node examples/consume.js --addr \"$DIAVASI_DATA_ADDR\" --ca \"$DIAVASI_CA\" --token \"$DIAVASI_API_TOKEN\" --group demo --consumer js --total 8"]
