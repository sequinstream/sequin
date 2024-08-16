defmodule Sequin.Bench.StatsTracker do
  @moduledoc false
  use Agent

  # Size of the reservoir
  @reservoir_size 1000
  # Interval for sampling means (in seconds)
  @sampling_interval 1
  # Number of recent samples to consider for stabilization
  @stabilization_sample_size 6
  # Maximum allowed standard deviation for stabilization
  @stabilization_std_dev_threshold 1.5

  def start_link([]) do
    Agent.start_link(
      fn ->
        %{
          count: 0,
          sum: 0,
          sum_squares: 0,
          min: nil,
          max: nil,
          reservoir: [],
          last_sampled_mean_at: System.monotonic_time(:second),
          sampled_means: [],
          mean_stabilized?: false,
          start_time: System.monotonic_time(:second)
        }
      end,
      name: __MODULE__
    )
  end

  def add_datapoint(agent, value) do
    Agent.get_and_update(agent, fn state ->
      current_time = System.monotonic_time(:second)

      new_count = state.count + 1
      new_sum = state.sum + value
      new_sum_squares = state.sum_squares + value * value
      new_reservoir = update_reservoir(state.reservoir, value, new_count)

      {new_sampled_means, new_last_sampled_at, new_mean_stabilized} =
        if current_time - state.last_sampled_mean_at >= @sampling_interval do
          new_mean = new_sum / new_count
          updated_sampled_means = Enum.take([new_mean | state.sampled_means], @stabilization_sample_size)
          {updated_sampled_means, current_time, check_stabilization(updated_sampled_means)}
        else
          {state.sampled_means, state.last_sampled_mean_at, state.mean_stabilized?}
        end

      new_state = %{
        count: new_count,
        sum: new_sum,
        sum_squares: new_sum_squares,
        min: min(state.min || value, value),
        max: max(state.max || value, value),
        reservoir: new_reservoir,
        last_sampled_mean_at: new_last_sampled_at,
        sampled_means: new_sampled_means,
        mean_stabilized?: new_mean_stabilized,
        start_time: state.start_time
      }

      {:ok, stats} = calculate_stats(new_state)
      {stats.mean_stabilized?, new_state}
    end)
  end

  def get_stats(agent) do
    Agent.get(agent, &calculate_stats/1)
  end

  defp calculate_stats(%{count: 0}) do
    {:error, "No data available - count is 0"}
  end

  defp calculate_stats(state) do
    avg = state.sum / state.count
    variance = state.sum_squares / state.count - avg * avg
    std_dev = :math.sqrt(variance)
    percentile_95 = percentile(state.reservoir, 95)

    {:ok,
     %{
       count: state.count,
       avg: avg,
       min: state.min,
       max: state.max,
       std_dev: std_dev,
       percentile_95: percentile_95,
       mean_stabilized?: state.mean_stabilized?
     }}
  end

  def stop(agent) do
    Agent.stop(agent)
  end

  defp update_reservoir(reservoir, value, count) when count <= @reservoir_size do
    [value | reservoir]
  end

  defp update_reservoir(reservoir, value, count) do
    if :rand.uniform(count) <= @reservoir_size do
      index = :rand.uniform(@reservoir_size) - 1
      List.replace_at(reservoir, index, value)
    else
      reservoir
    end
  end

  defp percentile(list, p) when is_list(list) and length(list) > 0 do
    sorted = Enum.sort(list)
    len = length(sorted)
    k = (len - 1) * p / 100
    f = floor(k)
    c = ceil(k)

    if f == c do
      Enum.at(sorted, f)
    else
      lower = Enum.at(sorted, f)
      upper = Enum.at(sorted, c)
      lower + (upper - lower) * (k - f)
    end
  end

  defp percentile(_, _), do: nil

  defp check_stabilization(sampled_means) when length(sampled_means) >= @stabilization_sample_size do
    avg = Enum.sum(sampled_means) / length(sampled_means)
    variance = Enum.reduce(sampled_means, 0, fn x, acc -> acc + (x - avg) * (x - avg) end) / length(sampled_means)
    std_dev = :math.sqrt(variance)

    Enum.all?(sampled_means, fn mean ->
      abs(mean - avg) <= @stabilization_std_dev_threshold * std_dev
    end)
  end

  defp check_stabilization(_), do: false
end
