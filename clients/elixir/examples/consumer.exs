opts = [
  addr: System.get_env("DIAVASI_DATA_ADDR", "127.0.0.1:7710"),
  ca: System.get_env("DIAVASI_CA", "/tmp/diavasi-sdk/dataplane-ca.crt"),
  token: System.get_env("DIAVASI_API_TOKEN", "sdk-demo"),
  group: System.get_env("DIAVASI_GROUP", "demo"),
  consumer: "elixir",
  max_in_flight: 1,
  total: 8
]

defmodule Demo.Consumer do
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def init(opts) do
    case Diavasi.Data.Client.start_link(opts) do
      {:ok, client} ->
        send(self(), :pull)
        {:ok, %{client: client, seen: 0, total: Keyword.fetch!(opts, :total)}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  def handle_info(:pull, state) do
    case Diavasi.Data.Client.next_batch(state.client) do
      {:ok, batch} ->
        Enum.each(batch.records, fn record ->
          IO.puts(
            "batch #{batch.batch_id} record #{record.record_id} (#{byte_size(record.payload)} bytes)"
          )
        end)

        :ok = Diavasi.Data.Client.ack(state.client, batch.batch_id)
        seen = state.seen + length(batch.records)

        if seen >= state.total do
          :ok = Diavasi.Data.Client.leave(state.client)
          {:stop, :normal, %{state | seen: seen}}
        else
          send(self(), :pull)
          {:noreply, %{state | seen: seen}}
        end

      :done ->
        {:stop, :normal, state}

      {:error, reason} ->
        IO.puts(:stderr, reason)
        {:stop, {:shutdown, reason}, state}
    end
  end
end

case Demo.Consumer.start_link(opts) do
  {:ok, pid} ->
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, :process, ^pid, :normal} ->
        :ok

      {:DOWN, ^ref, :process, ^pid, reason} ->
        IO.puts(:stderr, inspect(reason))
        System.halt(1)
    end

  {:error, reason} ->
    IO.puts(:stderr, "connect failed: #{inspect(reason)}")
    System.halt(1)
end
