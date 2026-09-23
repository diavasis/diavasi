defmodule Mix.Tasks.Diavasi.Consume do
  use Mix.Task

  @shortdoc "Consume a synthetic group over the TLS gRPC data plane"

  def run(args) do
    Mix.Task.run("app.start")

    {opts, _} =
      OptionParser.parse!(args,
        strict: [
          addr: :string,
          ca: :string,
          token: :string,
          group: :string,
          consumer: :string,
          total: :integer,
          max_in_flight: :integer
        ]
      )

    case Diavasi.Data.Client.run(opts) do
      {:ok, _ids} ->
        :ok

      {:error, reason} ->
        Mix.shell().error(reason)
        exit({:shutdown, 1})
    end
  end
end
