defmodule Sequin.Metrics.EctoBucket do
  @moduledoc false
  use Peep.Buckets.Custom,
    buckets: [1, 10, 100, 1000, 10_000]
end
