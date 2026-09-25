opts = [
  addr: System.get_env("DIAVASI_DATA_ADDR", "127.0.0.1:7710"),
  ca: System.get_env("DIAVASI_CA", "/tmp/diavasi-sdk/dataplane-ca.crt"),
  token: System.get_env("DIAVASI_API_TOKEN", "sdk-demo"),
  group: System.get_env("DIAVASI_GROUP", "demo"),
  consumer: "elixir-flow",
  max_in_flight: 1,
  total: 8
]

case Diavasi.Data.Client.start_link(opts) do
  {:ok, pid} ->
    try do
      pid
      |> Diavasi.Data.Client.stream()
      |> Stream.transform(0, fn batch, seen ->
        if seen >= opts[:total], do: {:halt, seen}, else: {[batch], seen + length(batch.records)}
      end)
      |> Flow.from_enumerable(stages: 1, max_demand: 1)
      |> Flow.map(fn batch ->
        Enum.each(batch.records, fn record ->
          IO.puts(
            "batch #{batch.batch_id} record #{record.record_id} (#{byte_size(record.payload)} bytes)"
          )
        end)

        :ok = Diavasi.Data.Client.ack(pid, batch.batch_id)
        batch
      end)
      |> Flow.run()
    rescue
      e in RuntimeError ->
        IO.puts(:stderr, Exception.message(e))
        System.halt(1)
    after
      if Process.alive?(pid), do: Diavasi.Data.Client.leave(pid)
    end

  {:error, reason} ->
    IO.puts(:stderr, "connect failed: #{inspect(reason)}")
    System.halt(1)
end
