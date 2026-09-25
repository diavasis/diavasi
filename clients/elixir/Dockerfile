FROM elixir:1.17
WORKDIR /app
COPY clients/elixir /app
RUN mix local.hex --force && mix deps.get
COPY clients/docker/wait-ca.sh /wait-ca.sh
RUN chmod +x /wait-ca.sh
CMD ["/bin/sh", "-c", "/wait-ca.sh && mix diavasi.consume --addr \"$DIAVASI_DATA_ADDR\" --ca \"$DIAVASI_CA\" --token \"$DIAVASI_API_TOKEN\" --group demo --consumer elixir --total 8"]
