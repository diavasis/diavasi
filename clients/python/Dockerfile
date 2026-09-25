FROM python:3.12-slim
RUN pip install --no-cache-dir grpcio protobuf
WORKDIR /app
COPY clients/python /app
COPY clients/docker/wait-ca.sh /wait-ca.sh
RUN chmod +x /wait-ca.sh
ENV PYTHONPATH=/app
CMD ["/bin/sh", "-c", "/wait-ca.sh && python -m diavasi_data --addr \"$DIAVASI_DATA_ADDR\" --ca \"$DIAVASI_CA\" --token \"$DIAVASI_API_TOKEN\" --group demo --consumer python --total 8"]
