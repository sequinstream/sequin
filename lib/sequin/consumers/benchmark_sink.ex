defmodule Sequin.Consumers.BenchmarkSink do
  @moduledoc """
  Sink configuration for benchmark consumers.

  Benchmark consumers are only allowed in dev/test environments and are used
  for end-to-end pipeline testing and benchmarking.
  """
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  @derive {Jason.Encoder, only: [:type, :partition_count]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:benchmark], default: :benchmark
    # Number of partitions for checksum tracking
    field :partition_count, :integer, default: System.schedulers_online()
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:partition_count])
    |> validate_number(:partition_count, greater_than: 0)
    |> ensure_not_production()
  end

  defp ensure_not_production(changeset) do
    if Application.get_env(:sequin, :env) == :prod do
      add_error(changeset, :type, "benchmark consumers are not allowed in production")
    else
      changeset
    end
  end
end
