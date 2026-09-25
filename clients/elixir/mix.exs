defmodule DiavasiBench.MixProject do
  use Mix.Project

  def project do
    [
      app: :diavasi_bench,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :ssl, :public_key]
    ]
  end

  defp deps do
    [
      {:protobuf, "~> 0.14"},
      {:mint, "~> 1.5"},
      {:jason, "~> 1.4"},
      {:flow, "~> 1.2"},
      {:gen_stage, "~> 1.2"},
      {:broadway, "~> 1.0"}
    ]
  end
end
