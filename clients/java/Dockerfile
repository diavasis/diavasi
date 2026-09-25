FROM gradle:8.10-jdk21
WORKDIR /workspace/clients/java
COPY clients/java /workspace/clients/java
COPY crates/diavasi/proto /workspace/crates/diavasi/proto
RUN gradle --no-daemon installDist
COPY clients/docker/wait-ca.sh /wait-ca.sh
RUN chmod +x /wait-ca.sh
CMD ["/bin/sh", "-c", "/wait-ca.sh && /workspace/clients/java/build/install/diavasi-data/bin/diavasi-data --addr \"$DIAVASI_DATA_ADDR\" --ca \"$DIAVASI_CA\" --token \"$DIAVASI_API_TOKEN\" --group demo --consumer java --total 8"]
