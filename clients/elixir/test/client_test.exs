defmodule Diavasi.Data.ClientTest do
  use ExUnit.Case

  test "consume acks every batch when a data plane is configured" do
    addr = System.get_env("DIAVASI_DATA_ADDR")
    ca = System.get_env("DIAVASI_CA")
    token = System.get_env("DIAVASI_API_TOKEN")

    if is_nil(addr) or is_nil(ca) or is_nil(token) do
      :ok
    else
      group = System.get_env("DIAVASI_GROUP") || "sdk"
      total = (System.get_env("DIAVASI_TOTAL") || "8") |> String.to_integer()

      assert {:ok, record_ids, _batch_ids} =
               Diavasi.Data.Client.run(
                 addr: addr,
                 ca: ca,
                 token: token,
                 group: group,
                 consumer: "elixir-test",
                 total: total
               )

      assert length(record_ids) == total
    end
  end
end
