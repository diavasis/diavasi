defmodule DiavasiBench.Envelope do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :version, 1, type: :uint32

  oneof :body, 0

  field :join_group, 2, type: DiavasiBench.JoinGroup, json_name: "joinGroup", oneof: 0
  field :joined, 3, type: DiavasiBench.Joined, oneof: 0
  field :record_batch, 4, type: DiavasiBench.RecordBatch, json_name: "recordBatch", oneof: 0
  field :ack, 5, type: DiavasiBench.Ack, oneof: 0
  field :flow_control, 6, type: DiavasiBench.FlowControl, json_name: "flowControl", oneof: 0
  field :heartbeat, 7, type: DiavasiBench.Heartbeat, oneof: 0
  field :error, 8, type: DiavasiBench.ErrorMessage, oneof: 0
end

defmodule DiavasiBench.JoinGroup do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :group_id, 1, type: :string, json_name: "groupId"
  field :consumer_id, 2, type: :string, json_name: "consumerId"
end

defmodule DiavasiBench.Joined do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :group_id, 1, type: :string, json_name: "groupId"
  field :consumer_id, 2, type: :string, json_name: "consumerId"
end

defmodule DiavasiBench.Record do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :record_id, 1, type: :uint64, json_name: "recordId"
  field :payload, 2, type: :bytes
end

defmodule DiavasiBench.RecordBatch do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :batch_id, 1, type: :uint64, json_name: "batchId"
  field :records, 2, repeated: true, type: DiavasiBench.Record
  field :sent_at_unix_ns, 3, type: :int64, json_name: "sentAtUnixNs"
end

defmodule DiavasiBench.Ack do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :batch_id, 1, type: :uint64, json_name: "batchId"
end

defmodule DiavasiBench.FlowControl do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :max_in_flight, 1, type: :uint32, json_name: "maxInFlight"
end

defmodule DiavasiBench.Heartbeat do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3
end

defmodule DiavasiBench.ErrorMessage do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :message, 1, type: :string
end
