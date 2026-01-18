# CozoDB Sustained Load Benchmark Runner
#
# Usage:
#   mix run lib/benchmark_runner.exs [options]
#
# Options are passed as command line arguments in KEY=VALUE format:
#   mix run lib/benchmark_runner.exs duration=600 workers=100 workload=mixed_50_50
#
# Available options:
#   duration     - Duration in seconds (default: 600 = 10 minutes)
#   workers      - Number of concurrent workers (default: 100)
#   tables       - Number of tables (default: 10)
#   rows         - Rows per table (default: 10000)
#   value_size   - Size of value field in bytes (default: 1024)
#   workload     - Workload type: read_only, write_only, mixed_50_50, mixed_80_20_wr (default: mixed_50_50)
#   engine       - Database engine: rocksdb, sqlite, mem (default: rocksdb)
#   db_path      - Database path (default: /tmp/cozodb_benchmark_sustained)
#   report_dir   - Report output directory (default: ./benchmark_reports)

defmodule BenchmarkRunner do
  def parse_args(args) do
    args
    |> Enum.map(&String.split(&1, "="))
    |> Enum.filter(&(length(&1) == 2))
    |> Enum.map(fn [k, v] -> {String.to_atom(k), parse_value(k, v)} end)
    |> Keyword.new()
  end

  defp parse_value("duration", v), do: String.to_integer(v)
  defp parse_value("rampdown", v), do: String.to_integer(v)
  defp parse_value("workers", v), do: String.to_integer(v)
  defp parse_value("tables", v), do: String.to_integer(v)
  defp parse_value("rows", v), do: String.to_integer(v)
  defp parse_value("value_size", v), do: String.to_integer(v)
  defp parse_value("workload", v), do: String.to_atom(v)
  defp parse_value("engine", v), do: String.to_atom(v)
  defp parse_value(_, v), do: v

  def run(args) do
    opts = parse_args(args)

    # Map short option names to full config names
    config = [
      duration_seconds: Keyword.get(opts, :duration, 600),
      rampdown_seconds: Keyword.get(opts, :rampdown, 60),
      num_workers: Keyword.get(opts, :workers, 100),
      num_tables: Keyword.get(opts, :tables, 10),
      seed_rows: Keyword.get(opts, :rows, 10_000),
      value_size: Keyword.get(opts, :value_size, 1024),
      workload: Keyword.get(opts, :workload, :mixed_50_50),
      engine: Keyword.get(opts, :engine, :rocksdb),
      db_path: Keyword.get(opts, :db_path, "/tmp/cozodb_benchmark_sustained"),
      report_dir: Keyword.get(opts, :report_dir, "./benchmark_reports")
    ]

    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("CozoDB Sustained Load Benchmark")
    IO.puts(String.duplicate("=", 60))
    IO.puts("\nConfiguration:")
    Enum.each(config, fn {k, v} ->
      IO.puts("  #{k}: #{inspect(v)}")
    end)
    IO.puts("")

    {:ok, report_path, metrics} = CozodbBenchmark.run(config)

    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("Benchmark Complete!")
    IO.puts(String.duplicate("=", 60))
    IO.puts("\nResults Summary:")
    IO.puts("  Total Operations: #{metrics.total_ops}")
    IO.puts("  Total Reads: #{metrics.total_reads}")
    IO.puts("  Total Writes: #{metrics.total_writes}")
    IO.puts("  Total Errors: #{metrics.total_errors}")

    if map_size(metrics.errors_by_type) > 0 do
      IO.puts("\nErrors by Type:")
      Enum.each(metrics.errors_by_type, fn {type, count} ->
        IO.puts("  #{type}: #{count}")
      end)
    end

    IO.puts("\nReport generated: #{report_path}")
    IO.puts("\nOpen the report in your browser:")
    IO.puts("  open #{report_path}")
  end
end

BenchmarkRunner.run(System.argv())
