defmodule Mix.Tasks.Bench do
  @moduledoc false
  use Mix.Task

  alias Sequin.Bench.EndToEnd
  alias Sequin.Streams

  def run(args) do
    Application.ensure_all_started(:sequin)

    {opts, [], []} =
      OptionParser.parse(args, strict: [suite: :string, truncate: :boolean, out_dir: :string])

    suite = Keyword.get(opts, :suite)
    truncate = Keyword.get(opts, :truncate)
    out_dir = Keyword.get(opts, :out_dir)

    if truncate && Application.get_env(:sequin, :env) == :dev do
      IO.puts("Truncating messages")
      Sequin.Repo.query!("TRUNCATE TABLE #{Streams.stream_schema()}.messages RESTART IDENTITY CASCADE")
    end

    opts = Sequin.Keyword.put_if_present([], :out_dir, out_dir)

    case suite do
      "end_to_end" -> EndToEnd.run(opts)
      "throughput" -> EndToEnd.run_throughput(opts)
      _ -> Mix.raise("Unknown suite: #{suite}")
    end
  end
end
