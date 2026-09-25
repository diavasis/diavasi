# Elixir client

`Diavasi.Data.Client` is a supervised consumer of `diavasi.data.v1`. It opens a TLS stream, sends the bearer token, Hello version 1, then JoinGroup. `stream/1` yields batches. The caller acks with `ack/2`. `leave/1` is the clean stop. Dropping the process returns unacked batches to the server. The client stores no cursor and does not dedupe on `record_id`. Reconnect with the same consumer id and the server replays them.

The Hex package name `diavasi` is reserved and unpublished. The Mix app in this tree is `:diavasi_bench`.

## Install

```elixir
{:diavasi_bench, path: "clients/elixir"}
```

## Library

```elixir
{:ok, pid} = Diavasi.Data.Client.start_link(
  addr: "127.0.0.1:7710",
  ca: "/tmp/diavasi-sdk/dataplane-ca.crt",
  token: "sdk-demo",
  group: "demo",
  consumer: "elixir",
  max_in_flight: 1
)

pid
|> Diavasi.Data.Client.stream()
|> Enum.each(fn batch ->
  IO.inspect(batch.batch_id)
  Diavasi.Data.Client.ack(pid, batch.batch_id)
end)

Diavasi.Data.Client.leave(pid)
```

`run/1` consumes `:total` records and acks each batch. `:halt_after` closes after that many acks and does not send Leave. `disconnect/1` closes the stream the same way.

`start_link/1` returns `{:error, reason}` when the handshake fails. A bad token is a gRPC unauthorized error. `next_batch/1` returns `{:ok, batch}`, `:done`, or `{:error, reason}`. `stream/1` raises that reason. A group that is not running is `protocol error 5`. Protocol codes are 1 bad version, 2 bad state, 3 unknown ack, 4 duplicate ack, 5 group not running, 6 unsupported, 7 internal, 8 heartbeat timeout.

Each example below uses the same connection options. Recreate the group before running a second one. The synthetic group `demo` has eight records, so these snippets stop after eight.

```elixir
opts = [
  addr: "127.0.0.1:7710",
  ca: "/tmp/diavasi-sdk/dataplane-ca.crt",
  token: "sdk-demo",
  group: "demo",
  consumer: "elixir",
  max_in_flight: 1,
  total: 8
]
```

`Diavasi.Data.Client` is already a GenServer. The modules below are the process that owns it: they pull batches, print each record, ack, and stop on an error.

### GenServer

```elixir
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
      {:DOWN, ^ref, :process, ^pid, :normal} -> :ok
      {:DOWN, ^ref, :process, ^pid, reason} -> IO.puts(:stderr, inspect(reason))
    end

  {:error, reason} ->
    IO.puts(:stderr, "connect failed: #{inspect(reason)}")
end
```

### Task

`Task.async/1` runs one consume off the caller. `Task.await/2` returns the error string when `start_link/1` or `next_batch/1` fails.

```elixir
task =
  Task.async(fn ->
    case Diavasi.Data.Client.start_link(Keyword.put(opts, :consumer, "elixir-task")) do
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
        after
          if Process.alive?(pid), do: Diavasi.Data.Client.leave(pid)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end)

case Task.await(task, 30_000) do
  {:error, reason} -> IO.puts(:stderr, inspect(reason))
  seen -> IO.puts("acked #{seen} records")
end
```

`stream/1` raises a protocol or gRPC error from `next_batch/1`. Wrap the `Enum` call in `try/rescue` when that task should return the message instead of crashing:

```elixir
try do
  pid |> Diavasi.Data.Client.stream() |> Enum.each(fn batch ->
    :ok = Diavasi.Data.Client.ack(pid, batch.batch_id)
  end)
rescue
  e in RuntimeError -> IO.puts(:stderr, Exception.message(e))
end
```

### Flow

Add `{:flow, "~> 1.2"}` to the application `mix.exs`. Keep one stage and `max_demand: 1` so the client acks one batch before the next pull.

```elixir
{:ok, pid} = Diavasi.Data.Client.start_link(Keyword.put(opts, :consumer, "elixir-flow"))

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

Diavasi.Data.Client.leave(pid)
```

### GenStage

Add `{:gen_stage, "~> 1.2"}`. The producer pulls batches. The consumer processes each record and acks. Demand is 1, so the next batch waits until that ack returns.

```elixir
defmodule Diavasi.Examples.Producer do
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

  def handle_demand(demand, state) when demand > 0 do
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

defmodule Diavasi.Examples.StageConsumer do
  use GenStage

  def start_link(producer), do: GenStage.start_link(__MODULE__, producer)

  def init(producer) do
    {:consumer, nil, subscribe_to: [{producer, max_demand: 1}]}
  end

  def handle_events(events, _from, state) do
    Enum.each(events, fn %{client: client, batch: batch} ->
      Enum.each(batch.records, fn record ->
        IO.puts(
          "batch #{batch.batch_id} record #{record.record_id} (#{byte_size(record.payload)} bytes)"
        )
      end)

      :ok = Diavasi.Data.Client.ack(client, batch.batch_id)
    end)

    {:noreply, [], state}
  end
end

{:ok, producer} =
  Diavasi.Examples.Producer.start_link(Keyword.put(opts, :consumer, "elixir-stage"))

{:ok, _consumer} = Diavasi.Examples.StageConsumer.start_link(producer)
```

### Broadway

Add `{:broadway, "~> 1.0"}` and `{:gen_stage, "~> 1.2"}`. The producer is a GenStage. `handle_message/3` prints each record and acks before the batch is marked successful.

```elixir
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
    message
  end
end

{:ok, pipeline} =
  Diavasi.Examples.Pipeline.start_link(Keyword.put(opts, :consumer, "elixir-broadway"))
```

`handle_failed/2` is the Broadway hook when `handle_message/3` raises. The ack in `handle_message/3` has already run only when the function returns the message. A raise before `ack/2` leaves the batch unacked, and the server replays it on the next join with the same consumer id.

## Run

Start the server from the repo root:

```bash
cargo build -p diavasi-cli
export PATH="$PWD/target/debug:$PATH"
mkdir -p /tmp/diavasi-sdk
diavasi serve --bind 127.0.0.1:7700 --data-bind 127.0.0.1:7710 \
  --store /tmp/diavasi-sdk/state --token sdk-demo
```

In a second terminal, from the repo root, create the group and run one script. Delete and create the group again before the next script, because each one consumes all eight records.

```bash
curl -fsS -X DELETE -H "Authorization: Bearer sdk-demo" \
  http://127.0.0.1:7700/v1/groups/demo || true
curl -fsS -H "Authorization: Bearer sdk-demo" -H "content-type: application/json" \
  -d '{"group_id":"demo","total_records":8,"payload_size":8,"max_buffer_records":64,"max_buffer_bytes":65536,"batch_max_records":4,"batch_timeout_ms":200,"ordering_contract":"synthetic-u64"}' \
  http://127.0.0.1:7700/v1/groups
curl -fsS -X POST -H "Authorization: Bearer sdk-demo" \
  http://127.0.0.1:7700/v1/groups/demo/start

cd clients/elixir
mix deps.get
mix run examples/consumer.exs
```

The other scripts are `examples/task.exs`, `examples/flow.exs`, `examples/gen_stage.exs`, and `examples/broadway.exs`. Flow, GenStage, and Broadway are dependencies of this Mix project so those scripts compile.

The flag client is still:

```bash
cd clients/elixir
mix diavasi.consume --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \
  --token sdk-demo --group demo --consumer elixir --total 8
```

Flags: `--addr`, `--ca`, `--token`, `--group`, `--consumer`, `--total`, `--max-in-flight` (default 1), `--halt-after`. The task prints `record_ids` and `batch_ids`.

```bash
docker compose -f clients/docker-compose.yml --profile elixir up --abort-on-container-exit
```

Livebook notes are in `notebooks/`. The Compose `notebook` profile serves [notebooks/demo.livemd](notebooks/demo.livemd) on port 8080.

## Test

`mix test` passes without a server. With `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` set, it consumes `DIAVASI_TOTAL` records (default 8) from `DIAVASI_GROUP`.

## Stage 0 bench

The TCP bench task in this tree speaks a different protocol from `data.proto`.

```bash
mise install
cd clients/elixir
mise exec -- mix deps.get
```

```bash
# terminal 1
cargo run -p diavasi --bin diavasi-transport-bench --features transport-bench -- \
  --transport tcp --role server --listen 127.0.0.1:9800 --total-records 200 --smoke

# terminal 2
cd clients/elixir
mise exec -- mix diavasi.bench --connect 127.0.0.1:9800 --total-records 200 \
  --output ../../docs/bench/results.jsonl
```
