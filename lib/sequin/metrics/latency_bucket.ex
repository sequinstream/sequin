defmodule Sequin.Metrics.LatencyBucket do
  @moduledoc false
  use Peep.Buckets.Custom,
    buckets: [10, 100, 1000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000, 10_000_000, 50_000_000]
end
