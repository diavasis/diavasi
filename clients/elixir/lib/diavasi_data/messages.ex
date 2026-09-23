defmodule Diavasi.Data.V1.Hello do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :protocol_version, 1, type: :uint32, json_name: "protocolVersion"
end

defmodule Diavasi.Data.V1.HelloAck do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :protocol_version, 1, type: :uint32, json_name: "protocolVersion"
end

defmodule Diavasi.Data.V1.JoinGroup do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :group_id, 1, type: :string, json_name: "groupId"
  field :consumer_id, 2, type: :string, json_name: "consumerId"
end

defmodule Diavasi.Data.V1.Joined do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :group_id, 1, type: :string, json_name: "groupId"
  field :consumer_id, 2, type: :string, json_name: "consumerId"
end

defmodule Diavasi.Data.V1.Record do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :record_id, 1, type: :uint64, json_name: "recordId"
  field :payload, 2, type: :bytes
end

defmodule Diavasi.Data.V1.RecordBatch do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :batch_id, 1, type: :uint64, json_name: "batchId"
  field :records, 2, repeated: true, type: Diavasi.Data.V1.Record
end

defmodule Diavasi.Data.V1.Ack do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :batch_id, 1, type: :uint64, json_name: "batchId"
end

defmodule Diavasi.Data.V1.Nack do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :batch_id, 1, type: :uint64, json_name: "batchId"
end

defmodule Diavasi.Data.V1.Heartbeat do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3
end

defmodule Diavasi.Data.V1.FlowControl do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :max_in_flight, 1, type: :uint32, json_name: "maxInFlight"
end

defmodule Diavasi.Data.V1.ErrorMessage do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :code, 1, type: :uint32
  field :message, 2, type: :string
end

defmodule Diavasi.Data.V1.Leave do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3
end

defmodule Diavasi.Data.V1.Envelope do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :version, 1, type: :uint32

  oneof :body, 0

  field :hello, 2, type: Diavasi.Data.V1.Hello, oneof: 0
  field :hello_ack, 3, type: Diavasi.Data.V1.HelloAck, json_name: "helloAck", oneof: 0
  field :join_group, 4, type: Diavasi.Data.V1.JoinGroup, json_name: "joinGroup", oneof: 0
  field :joined, 5, type: Diavasi.Data.V1.Joined, oneof: 0
  field :record_batch, 6, type: Diavasi.Data.V1.RecordBatch, json_name: "recordBatch", oneof: 0
  field :ack, 7, type: Diavasi.Data.V1.Ack, oneof: 0
  field :nack, 8, type: Diavasi.Data.V1.Nack, oneof: 0
  field :heartbeat, 9, type: Diavasi.Data.V1.Heartbeat, oneof: 0
  field :flow_control, 10, type: Diavasi.Data.V1.FlowControl, json_name: "flowControl", oneof: 0
  field :error, 11, type: Diavasi.Data.V1.ErrorMessage, oneof: 0
  field :leave, 12, type: Diavasi.Data.V1.Leave, oneof: 0
end
