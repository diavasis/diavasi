defmodule Diavasi.Data.Client do
  @moduledoc """
  Supervised consumer for the `diavasi.data.v1` stream.

  Put `Diavasi.Data.Client` in a supervision tree. `stream/1` yields batches.
  The caller acks with `ack/2`. `leave/1` is the clean stop. Dropping the
  process, or `disconnect/1`, returns unacked batches to the server. This
  client does not store a cursor and does not dedupe on `record_id`.
  """

  use GenServer, restart: :transient

  alias Diavasi.Data.V1.{Ack, Envelope, FlowControl, Hello, JoinGroup, Leave}

  @path "/diavasi.data.v1.DataPlane/Consume"

  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc "Yield batches from a running client. Ack each `batch.batch_id`."
  def stream(pid) when is_pid(pid) do
    Stream.resource(
      fn -> :open end,
      fn
        :open ->
          case next_batch(pid) do
            {:ok, batch} -> {[batch], :open}
            :done -> {:halt, :open}
            {:error, reason} -> raise reason
          end

        other ->
          {:halt, other}
      end,
      fn _ -> :ok end
    )
  end

  def next_batch(pid), do: GenServer.call(pid, :next_batch, 60_000)
  def ack(pid, batch_id), do: GenServer.call(pid, {:ack, batch_id})
  def leave(pid), do: GenServer.call(pid, :leave)

  @doc "Close the stream without Leave so unacked batches are replayed."
  def disconnect(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid, :normal)
    :ok
  end

  @doc """
  Consume `total` records, acking each batch.

  `halt_after` closes after that many acks without Leave.
  """
  def run(opts) do
    total = Keyword.fetch!(opts, :total)
    halt_after = Keyword.get(opts, :halt_after)

    case start_link(opts) do
      {:ok, pid} ->
        try do
          take(pid, total, halt_after, [], [])
        after
          if Process.alive?(pid), do: GenServer.stop(pid)
        end

      {:error, reason} ->
        {:error, format_error(reason)}
    end
  end

  def init(opts) do
    addr = Keyword.fetch!(opts, :addr)
    ca = Keyword.fetch!(opts, :ca)
    token = Keyword.fetch!(opts, :token)
    group = Keyword.fetch!(opts, :group)
    consumer = Keyword.get(opts, :consumer, "elixir")
    max_in_flight = Keyword.get(opts, :max_in_flight, 1)

    [host, port_s] = String.split(addr, ":")
    port = String.to_integer(port_s)

    with {:ok, conn} <-
           Mint.HTTP.connect(:https, host, port,
             protocols: [:http2],
             mode: :passive,
             transport_opts: [
               verify: :verify_peer,
               cacertfile: String.to_charlist(ca),
               server_name_indication: ~c"localhost"
             ]
           ),
         {:ok, conn, ref} <-
           Mint.HTTP.request(
             conn,
             "POST",
             @path,
             [
               {"content-type", "application/grpc"},
               {"te", "trailers"},
               {"authorization", "Bearer #{token}"}
             ],
             :stream
           ) do
      state = %{
        conn: conn,
        ref: ref,
        buffer: <<>>,
        pending: [],
        closed: false,
        group: group,
        consumer: consumer,
        max_in_flight: max_in_flight,
        sent_flow: false,
        left: false
      }

      state = send_env(state, hello())

      case handshake(state) do
        {:ok, state} ->
          {:ok, state}

        {:error, reason, state} ->
          Mint.HTTP.close(state.conn)
          {:stop, format_error(reason)}
      end
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  def handle_call(:next_batch, _from, state) do
    case pull(state) do
      {:batch, batch, state} -> {:reply, {:ok, batch}, state}
      {:done, state} -> {:reply, :done, state}
      {:error, reason, state} -> {:reply, {:error, format_error(reason)}, state}
    end
  end

  def handle_call({:ack, batch_id}, _from, state) do
    {:reply, :ok, send_env(state, %Envelope{version: 1, body: {:ack, %Ack{batch_id: batch_id}}})}
  end

  def handle_call(:leave, _from, state) do
    state = send_env(state, %Envelope{version: 1, body: {:leave, %Leave{}}})
    {:ok, conn} = Mint.HTTP.stream_request_body(state.conn, state.ref, :eof)
    {:reply, :ok, %{state | conn: conn, left: true}}
  end

  def terminate(_reason, %{conn: conn}) do
    Mint.HTTP.close(conn)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  defp take(pid, total, halt_after, record_ids, batch_ids) do
    if length(record_ids) >= total do
      :ok = leave(pid)
      {:ok, record_ids, batch_ids}
    else
      case next_batch(pid) do
        {:ok, batch} ->
          ids = Enum.map(batch.records, & &1.record_id)
          :ok = ack(pid, batch.batch_id)
          batch_ids = batch_ids ++ [batch.batch_id]
          record_ids = record_ids ++ ids

          if is_integer(halt_after) and length(batch_ids) >= halt_after do
            try do
              GenServer.call(pid, :next_batch, 5_000)
            catch
              :exit, _ -> :ok
            end

            disconnect(pid)
            {:ok, record_ids, batch_ids}
          else
            take(pid, total, halt_after, record_ids, batch_ids)
          end

        :done ->
          {:error,
           "incomplete consume records=#{length(record_ids)} batches=#{length(batch_ids)}"}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp handshake(state) do
    case pull_until(state, :joined) do
      {:ok, state} -> {:ok, state}
      other -> other
    end
  end

  defp pull_until(state, :joined) do
    case recv_events(state) do
      {:error, reason, state} ->
        {:error, reason, state}

      {:events, events, state} ->
        case apply_handshake(events, state) do
          {:joined, state} -> {:ok, state}
          {:continue, state} -> pull_until(state, :joined)
          {:error, reason, state} -> {:error, reason, state}
        end
    end
  end

  defp apply_handshake([], state), do: {:continue, state}

  defp apply_handshake([{:hello_ack, _} | rest], state) do
    state =
      send_env(
        state,
        %Envelope{
          version: 1,
          body: {:join_group, %JoinGroup{group_id: state.group, consumer_id: state.consumer}}
        }
      )

    apply_handshake(rest, state)
  end

  defp apply_handshake([{:joined, _} | rest], %{sent_flow: false} = state) do
    state = %{state | sent_flow: true}

    state =
      send_env(
        state,
        %Envelope{
          version: 1,
          body: {:flow_control, %FlowControl{max_in_flight: state.max_in_flight}}
        }
      )

    {batches, state, terminal} = collect(rest, state, [])

    state =
      state
      |> Map.put(:pending, Enum.reverse(batches))
      |> Map.put(:closed, terminal == :done)

    case terminal do
      {:error, reason} -> {:error, reason, state}
      _ -> {:joined, state}
    end
  end

  defp apply_handshake([{:heartbeat, _} | rest], state) do
    apply_handshake(rest, send_env(state, heartbeat()))
  end

  defp apply_handshake([{:error, err} | _], state) do
    {:error, {:protocol, err.code, err.message}, state}
  end

  defp apply_handshake([{:grpc, status, message} | _], state) do
    {:error, {:grpc, status, message}, state}
  end

  defp apply_handshake([{:done, _} | _], state), do: {:error, "stream closed", state}

  defp apply_handshake([other | _], state) do
    {:error, "unexpected frame #{inspect(other)}", state}
  end

  defp pull(%{closed: true, pending: []} = state), do: {:done, state}

  defp pull(%{pending: [batch | rest]} = state) do
    {:batch, batch, %{state | pending: rest}}
  end

  defp pull(state) do
    case recv_events(state) do
      {:error, reason, state} ->
        {:error, reason, state}

      {:events, events, state} ->
        {batches, state, terminal} = collect(events, state, [])
        state = if terminal == :done, do: %{state | closed: true}, else: state

        cond do
          match?({:error, _}, terminal) ->
            {:error, elem(terminal, 1), state}

          batches != [] ->
            [batch | rest] = Enum.reverse(batches)
            {:batch, batch, %{state | pending: rest}}

          terminal == :done ->
            {:done, state}

          true ->
            pull(state)
        end
    end
  end

  defp collect([], state, batches), do: {batches, state, nil}

  defp collect([{:record_batch, batch} | rest], state, batches) do
    collect(rest, state, [batch | batches])
  end

  defp collect([{:heartbeat, _} | rest], state, batches) do
    collect(rest, send_env(state, heartbeat()), batches)
  end

  defp collect([{:error, err} | _], state, _batches) do
    {[], state, {:error, {:protocol, err.code, err.message}}}
  end

  defp collect([{:grpc, status, message} | _], state, _batches) do
    {[], state, {:error, {:grpc, status, message}}}
  end

  defp collect([{:done, _} | _], state, batches), do: {batches, state, :done}

  defp collect([_ | rest], state, batches), do: collect(rest, state, batches)

  defp recv_events(state) do
    case Mint.HTTP.recv(state.conn, 0, 30_000) do
      {:ok, conn, responses} ->
        state = %{state | conn: conn}

        {events, state} =
          Enum.reduce(responses, {[], state}, fn response, {events, state} ->
            {more, state} = events_from(response, state)
            {events ++ more, state}
          end)

        events =
          if Enum.any?(events, &match?({:done, _}, &1)) and events == [] do
            events
          else
            events
          end

        {:events, events, state}

      {:error, conn, reason, _} ->
        {:error, "http2 error #{inspect(reason)}", %{state | conn: conn}}
    end
  end

  defp events_from({:status, _ref, status}, state) when status >= 400 do
    {[{:grpc, Integer.to_string(status), ""}], state}
  end

  defp events_from({:data, _ref, data}, state) do
    {frames, rest} = take_frames(state.buffer <> data, [])
    state = %{state | buffer: rest}
    {Enum.map(frames, &frame_event/1), state}
  end

  defp events_from({:headers, _ref, headers}, state) do
    case List.keyfind(headers, "grpc-status", 0) do
      {_, "0"} ->
        {[], state}

      {_, status} ->
        message =
          case List.keyfind(headers, "grpc-message", 0) do
            {_, msg} -> URI.decode(msg)
            nil -> ""
          end

        {[{:grpc, status, message}], state}

      nil ->
        {[], state}
    end
  end

  defp events_from({:done, _ref}, state), do: {[{:done, true}], state}
  defp events_from(_other, state), do: {[], state}

  defp frame_event(%Envelope{body: {:hello_ack, ack}}), do: {:hello_ack, ack}
  defp frame_event(%Envelope{body: {:joined, joined}}), do: {:joined, joined}
  defp frame_event(%Envelope{body: {:record_batch, batch}}), do: {:record_batch, batch}
  defp frame_event(%Envelope{body: {:heartbeat, beat}}), do: {:heartbeat, beat}
  defp frame_event(%Envelope{body: {:error, err}}), do: {:error, err}
  defp frame_event(other), do: {:other, other}

  defp hello, do: %Envelope{version: 1, body: {:hello, %Hello{protocol_version: 1}}}
  defp heartbeat, do: %Envelope{version: 1, body: {:heartbeat, %Diavasi.Data.V1.Heartbeat{}}}

  defp send_env(%{conn: conn, ref: ref} = state, envelope) do
    {:ok, conn} = Mint.HTTP.stream_request_body(conn, ref, frame(envelope))
    %{state | conn: conn}
  end

  defp frame(envelope) do
    bin = Envelope.encode(envelope)
    <<0, byte_size(bin)::32-big, bin::binary>>
  end

  defp take_frames(<<_flag, len::32-big, rest::binary>>, acc) when byte_size(rest) >= len do
    <<payload::binary-size(^len), rest::binary>> = rest
    take_frames(rest, [Envelope.decode(payload) | acc])
  end

  defp take_frames(buffer, acc), do: {Enum.reverse(acc), buffer}

  defp format_error({:protocol, code, message}), do: "protocol error #{code}: #{message}"
  defp format_error({:grpc, status, message}), do: "grpc #{status}: #{message}"
  defp format_error(other), do: to_string_error(other)

  defp to_string_error(reason) when is_binary(reason), do: reason
  defp to_string_error(reason), do: inspect(reason)
end
