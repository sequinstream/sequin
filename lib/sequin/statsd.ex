defmodule Sequin.Statsd do
  @moduledoc false
  # Module interface pulled from Statix
  @type key :: binary()
  @type sample_rate :: integer()
  @type options :: [tags: map()]
  @type on_send :: :ok | {:error, term}

  defguard up_to_100(value) when is_integer(value) and value > 0 and value <= 100

  @doc """
  Measures the execution time of the given `function` and writes that to the
  StatsD timing identified by `key`.
  This function returns the value returned by `function`.
  """
  @spec measure(key(), fun()) :: on_send() | term()
  @spec measure(key(), options(), fun()) :: on_send() | term()
  def measure(key, options \\ [], fun) when is_function(fun, 0) do
    {elapsed, result} = :timer.tc(fun)
    timing(key, div(elapsed, 1000), options)
    result
  end

  @doc """
  Like measure/3, but only sends the result to Datadog as often as `rate` is set.
  `rate` of 50 means roughly half the measures will be sent to Datadog. 100 means all
  measures will be sent.
  """
  @spec sample_measure(key(), sample_rate(), fun()) :: on_send() | term()
  @spec sample_measure(key(), sample_rate(), options(), fun()) :: on_send() | term()
  def sample_measure(key, rate, opts \\ [], fun) when is_function(fun, 0) and up_to_100(rate) do
    if sample?(rate, opts), do: measure(key, opts, fun), else: fun.()
  end

  @doc """
  Writes the given `value` to the StatsD timing identified by `key`.
  `value` is expected in milliseconds.
  """
  @spec timing(key(), String.Chars.t()) :: on_send()
  @spec timing(key(), String.Chars.t(), options()) :: on_send()
  def timing(key, val, options \\ []) do
    :dogstatsd.timer(key, val, tags_from_opts(options))
  end

  @doc """
  Increments the StatsD counter identified by `key` by the given `value`.
  `value` is supposed to be zero or positive and `c:decrement/3` should be
  used for negative values.
  """
  @spec increment(key()) :: term()
  @spec increment(key(), number()) :: term()
  @spec increment(key(), number(), options()) :: term()
  def increment(key, val \\ 1, options \\ []) when is_number(val) do
    :dogstatsd.increment(key, val, tags_from_opts(options))
  end

  @doc """
  Like increment/3, but only sends the result to Datadog as often as `rate` is set.
  `rate` of 50 means roughly half the measures will be sent to Datadog. 100 means all
  measures will be sent.
  """
  @spec sample_increment(key(), sample_rate()) :: term()
  @spec sample_increment(key(), sample_rate(), number()) :: term()
  @spec sample_increment(key(), sample_rate(), number(), options()) :: term()
  def sample_increment(key, rate, val \\ 1, opts \\ []) when up_to_100(rate) and is_number(val) do
    if sample?(rate, opts), do: increment(key, val, opts)
  end

  @spec gauge(key(), number()) :: term()
  @spec gauge(key(), number(), options()) :: term()
  def gauge(key, val, options \\ []) when is_number(val) do
    :dogstatsd.gauge(key, val, tags_from_opts(options))
  end

  @spec histogram(key(), number()) :: term()
  @spec histogram(key(), number(), options()) :: term()
  def histogram(key, val, options \\ []) do
    :dogstatsd.histogram(key, val, tags_from_opts(options))
  end

  @spec put_tags(map()) :: :ok
  @spec put_tags(Keyword.t()) :: :ok
  def put_tags(tags) when is_list(tags) do
    tags |> Map.new() |> put_tags()
  end

  def put_tags(tags) when is_map(tags) do
    tags = Map.merge(get_tags(), tags)
    Process.put(:statsd_tags, tags)
  end

  defp get_tags do
    Process.get(:statsd_tags, %{})
  end

  defp tags_from_opts(opts) do
    process_tags = get_tags()
    opts_tags = Keyword.get(opts, :tags, %{})

    Map.merge(process_tags, opts_tags)
  end

  defp sample?(rate, _opts) do
    :rand.uniform(100) <= rate
  end
end
