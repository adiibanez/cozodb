defmodule CozodbBenchmark.MixProject do
  use Mix.Project

  def project do
    [
      app: :cozodb_benchmark,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {CozodbBenchmark.Application, []}
    ]
  end

  defp deps do
    [
      {:cozodb, path: "../.."},
      {:benchee, "~> 1.3"},
      {:benchee_html, "~> 1.0"},
      {:benchee_markdown, "~> 0.3"},
      {:statistex, "~> 1.0"},
      {:jason, "~> 1.4"}
    ]
  end

  defp aliases do
    [
      benchmark: "run lib/benchmark_runner.exs"
    ]
  end
end
