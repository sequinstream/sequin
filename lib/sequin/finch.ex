defmodule Sequin.Finch do
  @moduledoc false
  def child_spec do
    pool_size =
      cond do
        finch_pool_size = config!(:pool_size) ->
          finch_pool_size

        default_workers = Application.get_env(:sequin, :default_workers_per_sink) ->
          default_workers * 1.5

        true ->
          # Sort out how big the machine is, use as a proxy for capability
          cores = System.schedulers_online()

          # Scaling factor:
          # 4-core machine: sqrt(4) * 30 = 2 * 30 = 60
          # 8-core machine: sqrt(8) * 30 = 2.83 * 30 ≈ 85
          # 16-core machine: sqrt(16) * 30 = 4 * 30 = 120
          # 32-core machine: sqrt(32) * 30 = 5.66 * 30 ≈ 170
          # 64-core machine: sqrt(64) * 30 = 8 * 30 = 240
          # 96-core machine: sqrt(96) * 30 = 9.8 * 30 ≈ 294
          base_size = :math.sqrt(cores) * 30

          min(400, max(50, trunc(base_size)))
      end

    pools = %{default: [size: pool_size, count: config!(:pool_count)]}

    {Finch, name: __MODULE__, pools: pools, pool_timeout: to_timeout(second: 10)}
  end

  defp config!(key) do
    Keyword.fetch!(config!(), key)
  end

  defp config! do
    Application.fetch_env!(:sequin, Sequin.Finch)
  end
end
