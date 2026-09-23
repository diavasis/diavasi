defmodule Diavasi.Data.Client do
  @moduledoc false

  alias Diavasi.Data.V1.{Ack, Envelope, ErrorMessage, FlowControl, Hello, JoinGroup, Leave}

  @path "/diavasi.data.v1.DataPlane/Consume"

  def run(opts) do
    addr = Keyword.fetch!(opts, :addr)
    ca = Keyword.fetch!(opts, :ca)
    token = Keyword.fetch!(opts, :token)
    group = Keyword.fetch!(opts, :group)
    consumer = Keyword.get(opts, :consumer, "elixir")
    total = Keyword.fetch!(opts, :total)
    max_in_flight = Keyword.get(opts, :max_in_flight, 4)
    halt_after = Keyword.get(opts, :halt_after)

    [host, port_s] = String.split(addr, ":")
    port = String.to_integer(port_s)

    {:ok, conn} =
      Mint.HTTP.connect(:https, host, port,
        protocols: [:http2],
        mode: :passive,
        transport_opts: [
          verify: :verify_peer,
          cacertfile: String.to_charlist(ca),
          server_name_indication: ~c"localhost"
        ]
      )

    headers = [
      {"content-type", "application/grpc"},
      {"te", "trailers"},
      {"authorization", "Bearer #{token}"}
    ]

    {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", @path, headers, :stream)
    conn = send_env(conn, ref, %Envelope{version: 1, body: {:hello, %Hello{protocol_version: 1}}})

    state = %{
      conn: conn,
      ref: ref,
      buffer: <<>>,
      group: group,
      consumer: consumer,
      total: total,
      max_in_flight: max_in_flight,
      seen: MapSet.new(),
      acked: 0,
      sent_flow: false,
      done: false,
      halted: false,
      halt_after: halt_after,
      error: nil
    }

    state = loop(state)

    unless state.halted do
      Mint.HTTP.close(state.conn)
    end

    ids = state.seen |> MapSet.to_list() |> Enum.sort()

    cond do
      state.error != nil ->
        {:error, state.error}

      state.halted ->
        {:ok, ids}

      MapSet.size(state.seen) == total and state.acked > 0 ->
        IO.puts("elixir consumed #{MapSet.size(state.seen)} records in #{state.acked} batches")
        {:ok, ids}

      true ->
        {:error, "incomplete consume seen=#{MapSet.size(state.seen)} acked=#{state.acked}"}
    end
  end

  defp loop(%{done: true} = state), do: state
  defp loop(%{error: error} = state) when error != nil, do: state

  defp loop(state) do
    case Mint.HTTP.recv(state.conn, 0, 30_000) do
      {:ok, conn, responses} ->
        state = %{state | conn: conn}
        state = Enum.reduce(responses, state, &apply_response/2)
        loop(state)

      {:error, conn, reason, _} ->
        %{state | conn: conn, error: "http2 error #{inspect(reason)}"}
    end
  end

  defp apply_response({:status, _ref, status}, state) when status >= 400 do
    %{state | error: "http status #{status}"}
  end

  defp apply_response({:data, _ref, data}, state) do
    {frames, rest} = take_frames(state.buffer <> data, [])
    state = %{state | buffer: rest}
    Enum.reduce(frames, state, &handle_frame/2)
  end

  defp apply_response({:headers, _ref, headers}, state) do
    case List.keyfind(headers, "grpc-status", 0) do
      {_, status} when status != "0" ->
        message =
          case List.keyfind(headers, "grpc-message", 0) do
            {_, msg} -> msg
            nil -> ""
          end

        %{state | error: "grpc status #{status} #{message}", done: true}

      _ ->
        state
    end
  end

  defp apply_response({:done, _ref}, state), do: %{state | done: true}
  defp apply_response(_other, state), do: state

  defp handle_frame(_frame, %{done: true} = state), do: state
  defp handle_frame(_frame, %{error: error} = state) when error != nil, do: state

  defp handle_frame(%Envelope{body: {:hello_ack, _}}, state) do
    send_env(
      state,
      %Envelope{
        version: 1,
        body: {:join_group, %JoinGroup{group_id: state.group, consumer_id: state.consumer}}
      }
    )
  end

  defp handle_frame(%Envelope{body: {:joined, _}}, %{sent_flow: false} = state) do
    state = %{state | sent_flow: true}

    send_env(
      state,
      %Envelope{
        version: 1,
        body: {:flow_control, %FlowControl{max_in_flight: state.max_in_flight}}
      }
    )
  end

  defp handle_frame(%Envelope{body: {:joined, _}}, state), do: state

  defp handle_frame(%Envelope{body: {:record_batch, batch}}, state) do
    seen =
      Enum.reduce(batch.records, state.seen, fn record, acc ->
        MapSet.put(acc, record.record_id)
      end)

    state = %{state | seen: seen}

    if is_integer(state.halt_after) and state.acked >= state.halt_after do
      Mint.HTTP.close(state.conn)
      %{state | done: true, halted: true}
    else
      ack_batch(state, batch)
    end
  end

  defp handle_frame(%Envelope{body: {:heartbeat, _}}, state), do: state

  defp handle_frame(%Envelope{body: {:error, %ErrorMessage{} = err}}, state) do
    %{state | error: "protocol error #{err.code}: #{err.message}", done: true}
  end

  defp handle_frame(other, state) do
    %{state | error: "unexpected frame #{inspect(other)}", done: true}
  end

  defp ack_batch(state, batch) do
    state = %{state | acked: state.acked + 1}
    state = send_env(state, %Envelope{version: 1, body: {:ack, %Ack{batch_id: batch.batch_id}}})

    if MapSet.size(state.seen) >= state.total do
      state = send_env(state, %Envelope{version: 1, body: {:leave, %Leave{}}})
      {:ok, conn} = Mint.HTTP.stream_request_body(state.conn, state.ref, :eof)
      %{state | conn: conn, done: true}
    else
      state
    end
  end

  defp send_env(%{conn: conn, ref: ref} = state, envelope) do
    {:ok, conn} = Mint.HTTP.stream_request_body(conn, ref, frame(envelope))
    %{state | conn: conn}
  end

  defp send_env(conn, ref, envelope) do
    {:ok, conn} = Mint.HTTP.stream_request_body(conn, ref, frame(envelope))
    conn
  end

  defp frame(envelope) do
    bin = Envelope.encode(envelope)
    <<0, byte_size(bin)::32-big, bin::binary>>
  end

  defp take_frames(<<flag, len::32-big, rest::binary>>, acc) when byte_size(rest) >= len do
    <<payload::binary-size(len), rest::binary>> = rest
    _ = flag
    frame = Envelope.decode(payload)
    take_frames(rest, [frame | acc])
  end

  defp take_frames(buffer, acc), do: {Enum.reverse(acc), buffer}
end
