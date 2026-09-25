parent = self()

opts = [
  addr: System.get_env("DIAVASI_DATA_ADDR", "127.0.0.1:7710"),
  ca: System.get_env("DIAVASI_CA", "/tmp/diavasi-sdk/dataplane-ca.crt"),
  token: System.get_env("DIAVASI_API_TOKEN", "sdk-demo"),
  group: System.get_env("DIAVASI_GROUP", "demo"),
  consumer: "elixir-stage",
  max_in_flight: 1,
  total: 8,
  notify: parent
]

defmodule Diavasi.Examples.Producer do
  use GenStage

  def start_link(opts), do: GenStage.start_link(__MODULE__, opts)

  def init(opts) do
    case Diavasi.Data.Client.start_link(opts) do
      {:ok, client} ->
        {:producer,
         %{
           client: client,
           seen: 0,
           total: Keyword.fetch!(opts, :total),
           notify: Keyword.fetch!(opts, :notify)
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  def handle_demand(_demand, %{seen: seen, total: total} = state) when seen >= total do
    {:noreply, [], state}
  end

  def handle_demand(demand, state) when demand > 0 do
    case Diavasi.Data.Client.next_batch(state.client) do
      {:ok, batch} ->
        event = %{client: state.client, batch: batch}
        {:noreply, [event], %{state | seen: state.seen + length(batch.records)}}

      :done ->
        send(state.notify, :done)
        {:noreply, [], state}

      {:error, reason} ->
        send(state.notify, {:error, reason})
        {:stop, reason, state}
    end
  end
end

defmodule Diavasi.Examples.StageConsumer do
  use GenStage

  def start_link(args), do: GenStage.start_link(__MODULE__, args)

  def init({producer, notify, total}) do
    {:consumer, %{notify: notify, total: total, seen: 0},
     subscribe_to: [{producer, max_demand: 1}]}
  end

  def handle_events(events, _from, state) do
    seen =
      Enum.reduce(events, state.seen, fn %{client: client, batch: batch}, seen ->
        Enum.each(batch.records, fn record ->
          IO.puts(
            "batch #{batch.batch_id} record #{record.record_id} (#{byte_size(record.payload)} bytes)"
          )
        end)

        :ok = Diavasi.Data.Client.ack(client, batch.batch_id)
        seen + length(batch.records)
      end)

    if seen >= state.total, do: send(state.notify, :done)
    {:noreply, [], %{state | seen: seen}}
  end
end

case Diavasi.Examples.Producer.start_link(opts) do
  {:ok, producer} ->
    {:ok, _consumer} = Diavasi.Examples.StageConsumer.start_link({producer, parent, opts[:total]})

    receive do
      :done ->
        :ok

      {:error, reason} ->
        IO.puts(:stderr, inspect(reason))
        System.halt(1)
    after
      30_000 ->
        IO.puts(:stderr, "timed out waiting for records")
        System.halt(1)
    end

  {:error, reason} ->
    IO.puts(:stderr, "connect failed: #{inspect(reason)}")
    System.halt(1)
end
