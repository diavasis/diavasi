defmodule DiavasiBench.TcpClient do
  @moduledoc "Stage 0 TCP length-prefixed protobuf client."

  def run(opts \\ []) do
    connect = Keyword.get(opts, :connect, "127.0.0.1:9800")
    group_id = Keyword.get(opts, :group_id, "bench")
    total = Keyword.get(opts, :total_records, 200)
    max_in_flight = Keyword.get(opts, :max_in_flight, 2)
    output = Keyword.get(opts, :output)

    [host, port_s] = String.split(connect, ":")
    port = String.to_integer(port_s)
    {:ok, socket} = :gen_tcp.connect(String.to_charlist(host), port, [:binary, active: false])

    send_env(socket, %DiavasiBench.Envelope{
      version: 1,
      body: {:flow_control, %DiavasiBench.FlowControl{max_in_flight: max_in_flight}}
    })

    send_env(socket, %DiavasiBench.Envelope{
      version: 1,
      body:
        {:join_group,
         %DiavasiBench.JoinGroup{
           group_id: group_id,
           consumer_id: "elixir-#{System.system_time(:second)}"
         }}
    })

    t0 = System.monotonic_time(:nanosecond)
    {records, batches, bytes, joined} = loop(socket, total, 0, 0, 0, false)
    elapsed = max((System.monotonic_time(:nanosecond) - t0) / 1.0e9, 1.0e-9)

    result = %{
      transport: "tcp",
      client_lang: "elixir",
      consumers: 1,
      total_records: total,
      metrics: %{
        elapsed_secs: elapsed,
        records: records,
        bytes: bytes,
        batches: batches,
        records_per_sec: records / elapsed,
        mib_per_sec: bytes / (1024 * 1024) / elapsed
      },
      notes: ["transport=tcp", "joined=#{joined}"]
    }

    IO.puts(Jason.encode!(result, pretty: true))

    if output do
      File.mkdir_p!(Path.dirname(output))
      File.write!(output, Jason.encode!(result) <> "\n", [:append])
    end

    :gen_tcp.close(socket)
    :ok
  end

  defp loop(_socket, total, records, batches, bytes, joined) when records >= total do
    {records, batches, bytes, joined}
  end

  defp loop(socket, total, records, batches, bytes, joined) do
    env = recv_env(socket)

    case env.body do
      {:joined, _} ->
        loop(socket, total, records, batches, bytes, true)

      {:record_batch, batch} ->
        n = length(batch.records)
        b = Enum.reduce(batch.records, 0, fn r, acc -> acc + byte_size(r.payload) end)

        send_env(socket, %DiavasiBench.Envelope{
          version: 1,
          body: {:ack, %DiavasiBench.Ack{batch_id: batch.batch_id}}
        })

        loop(socket, total, records + n, batches + 1, bytes + b, joined)

      {:error, err} ->
        raise err.message

      _ ->
        loop(socket, total, records, batches, bytes, joined)
    end
  end

  defp send_env(socket, %DiavasiBench.Envelope{} = env) do
    payload = DiavasiBench.Envelope.encode(env)
    :ok = :gen_tcp.send(socket, <<byte_size(payload)::32>> <> payload)
  end

  defp recv_env(socket) do
    {:ok, <<len::32>>} = :gen_tcp.recv(socket, 4)
    {:ok, payload} = :gen_tcp.recv(socket, len)
    DiavasiBench.Envelope.decode(payload)
  end
end
