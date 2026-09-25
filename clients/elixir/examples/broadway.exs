defmodule Diavasi.Examples.Progress do
  def start_link, do: Agent.start_link(fn -> 0 end, name: __MODULE__)
  def add(n), do: Agent.update(__MODULE__, &(&1 + n))
  def count, do: Agent.get(__MODULE__, & &1)
end

opts = [
  addr: System.get_env("DIAVASI_DATA_ADDR", "127.0.0.1:7710"),
  ca: System.get_env("DIAVASI_CA", "/tmp/diavasi-sdk/dataplane-ca.crt"),
  token: System.get_env("DIAVASI_API_TOKEN", "sdk-demo"),
  group: System.get_env("DIAVASI_GROUP", "demo"),
  consumer: "elixir-broadway",
  max_in_flight: 1,
  total: 8
]

defmodule Diavasi.Examples.BroadwayProducer do
  use GenStage

  def start_link(opts), do: GenStage.start_link(__MODULE__, opts)

  def init(opts) do
    case Diavasi.Data.Client.start_link(opts) do
      {:ok, client} ->
        {:producer, %{client: client, seen: 0, total: Keyword.fetch!(opts, :total)}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  def handle_demand(_demand, %{seen: seen, total: total} = state) when seen >= total do
    {:noreply, [], state}
  end

  def handle_demand(_demand, state) do
    case Diavasi.Data.Client.next_batch(state.client) do
      {:ok, batch} ->
        event = %{client: state.client, batch: batch}
        {:noreply, [event], %{state | seen: state.seen + length(batch.records)}}

      :done ->
        {:noreply, [], state}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end
end

defmodule Diavasi.Examples.Pipeline do
  use Broadway

  def start_link(opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {Diavasi.Examples.BroadwayProducer, opts},
        concurrency: 1
      ],
      processors: [default: [concurrency: 1, max_demand: 1]]
    )
  end

  @impl true
  def handle_message(_processor, message, _context) do
    %{client: client, batch: batch} = message.data

    Enum.each(batch.records, fn record ->
      IO.puts(
        "batch #{batch.batch_id} record #{record.record_id} (#{byte_size(record.payload)} bytes)"
      )
    end)

    :ok = Diavasi.Data.Client.ack(client, batch.batch_id)
    Diavasi.Examples.Progress.add(length(batch.records))
    message
  end
end

{:ok, _} = Diavasi.Examples.Progress.start_link()

case Diavasi.Examples.Pipeline.start_link(opts) do
  {:ok, pipeline} ->
    deadline = System.monotonic_time(:millisecond) + 30_000

    wait = fn wait ->
      cond do
        Diavasi.Examples.Progress.count() >= opts[:total] ->
          Broadway.stop(pipeline)

        System.monotonic_time(:millisecond) > deadline ->
          IO.puts(:stderr, "timed out waiting for records")
          System.halt(1)

        true ->
          Process.sleep(50)
          wait.(wait)
      end
    end

    wait.(wait)

  {:error, reason} ->
    IO.puts(:stderr, "connect failed: #{inspect(reason)}")
    System.halt(1)
end
