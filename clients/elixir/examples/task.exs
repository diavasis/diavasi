opts = [
  addr: System.get_env("DIAVASI_DATA_ADDR", "127.0.0.1:7710"),
  ca: System.get_env("DIAVASI_CA", "/tmp/diavasi-sdk/dataplane-ca.crt"),
  token: System.get_env("DIAVASI_API_TOKEN", "sdk-demo"),
  group: System.get_env("DIAVASI_GROUP", "demo"),
  consumer: "elixir-task",
  max_in_flight: 1,
  total: 8
]

task =
  Task.async(fn ->
    case Diavasi.Data.Client.start_link(opts) do
      {:ok, pid} ->
        try do
          pid
          |> Diavasi.Data.Client.stream()
          |> Enum.reduce_while(0, fn batch, seen ->
            Enum.each(batch.records, fn record ->
              IO.puts(
                "batch #{batch.batch_id} record #{record.record_id} (#{byte_size(record.payload)} bytes)"
              )
            end)

            :ok = Diavasi.Data.Client.ack(pid, batch.batch_id)
            seen = seen + length(batch.records)
            if seen >= opts[:total], do: {:halt, seen}, else: {:cont, seen}
          end)
        rescue
          e in RuntimeError ->
            {:error, Exception.message(e)}
        after
          if Process.alive?(pid), do: Diavasi.Data.Client.leave(pid)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end)

case Task.await(task, 30_000) do
  {:error, reason} ->
    IO.puts(:stderr, inspect(reason))
    System.halt(1)

  seen ->
    IO.puts("acked #{seen} records")
end
