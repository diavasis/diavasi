defmodule Mix.Tasks.Diavasi.Bench do
  use Mix.Task

  @shortdoc "Run Stage 0 TCP bench client"

  def run(args) do
    {opts, _, _} =
      OptionParser.parse(args,
        strict: [
          connect: :string,
          group_id: :string,
          total_records: :integer,
          max_in_flight: :integer,
          output: :string
        ]
      )

    DiavasiBench.TcpClient.run(opts)
  end
end
