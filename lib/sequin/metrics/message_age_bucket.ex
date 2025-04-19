defmodule Sequin.Metrics.MessageAgeBucket do
  @moduledoc false
  use Peep.Buckets.Custom,
    buckets: [1000, 5000, 10_000, 30_000, 60_000, 300_000, 600_000, 1_800_000, 3_600_000]
end
