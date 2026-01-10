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
  end
end
