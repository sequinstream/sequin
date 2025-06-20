defmodule Sequin.EtsCache do
  @moduledoc """
  A TTL-based ETS cache with proactive warming capabilities.

  This module provides a simple caching layer backed by ETS with automatic expiration
  and proactive cache warming. It's designed to cache expensive operations while
  ensuring fresh data through configurable TTL and warming strategies.

  ## Features

  - **TTL-based expiration**: Cached values automatically expire after a configurable time
  - **Proactive warming**: Cache entries are refreshed before they expire to avoid cache misses
  - **Configurable warming function**: Custom logic for refreshing cache entries
  - **ETS-backed**: High-performance in-memory storage

  ## Usage

  First, define a cache configuration:

      @cache_config %EtsCache.Config{
        cache_name: :my_cache,
        ttl: :timer.seconds(90),
        warm_after: :timer.seconds(60),
        warmer: {__MODULE__, :warm_my_cache}
      }

  Then use the cache in your functions:

      def load_cached(resource_id) do
        data = EtsCache.lookup(resource_id, @cache_config, fn ->
          # Expensive operation to load data
          load_from_database(resource_id)
        end)

        {:ok, data}
      end

  Implement a warmer function to handle proactive cache refreshing:

      def warm_my_cache(resource_id) do
        case EtsCache.peek(resource_id, @cache_config) do
          nil ->
            # Cache miss - load fresh data
            with {:ok, data} <- load_from_database(resource_id) do
              EtsCache.insert(resource_id, data, @cache_config)
            end

          cached_data ->
            # Cache hit - decide if refresh is needed
            if needs_refresh?(cached_data) do
              with {:ok, fresh_data} <- load_from_database(resource_id) do
                EtsCache.insert(resource_id, fresh_data, @cache_config)
              end
            else
              # Re-insert to reset TTL
              EtsCache.insert(resource_id, cached_data, @cache_config)
            end
        end

        :ok
      end

  ## Cache Warming

  The cache warming mechanism works as follows:

  1. When a value is inserted via `insert/3`, a timer is set to call the warmer function
  2. The warmer is called after `warm_after` milliseconds
  3. The warmer can check if the cache entry still exists using `peek/2`
  4. The warmer can decide whether to refresh the data or just reset the TTL

  This ensures that frequently accessed cache entries are kept fresh without waiting
  for expiration and cache misses.

  ## Configuration

  The `Config` struct supports the following fields:

  - `cache_name`: The name of the ETS table (must be created beforehand)
  - `ttl`: Time-to-live in milliseconds
  - `warm_after`: Time in milliseconds after which to trigger warming
  - `warmer`: A `{module, function}` tuple for the warming function

  ## ETS Table Management

  The cache expects ETS tables to be created and managed externally. The module
  provides a `tables/0` function that returns the list of expected table names
  for reference.
  """
  @tables [:consumer_cache, :http_endpoint_cache, :database_cache, :cache_for_test]

  defmodule Config do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :cache_name, atom()
      field :ttl, number()
      field :warm_after, number()
      field :warmer, {module :: atom(), fun :: atom()}
    end
  end

  def init do
    Enum.each(@tables, fn table ->
      :ets.new(table, [:named_table, :public])
    end)
  end

  @spec lookup(any(), Config.t(), function()) :: any()
  def lookup(key, %Config{} = config, fun) do
    case :ets.lookup(config.cache_name, key) do
      [{_key, {value, expires_at}}] ->
        if DateTime.before?(expires_at, DateTime.utc_now()) do
          # Note that the return value of fun is not checked.
          # If you don't want to cache, raise or alter this behavior.
          value = fun.()
          insert(key, value, config)
          value
        else
          value
        end

      _ ->
        value = fun.()
        insert(key, value, config)
        value
    end
  end

  @spec peek(any(), Config.t()) :: nil | any()
  def peek(key, %Config{} = config) do
    case :ets.lookup(config.cache_name, key) do
      [{_key, {value, expires_at}}] ->
        if DateTime.before?(expires_at, DateTime.utc_now()) do
          value
        end

      _ ->
        nil
    end
  end

  @spec insert(any(), any(), Config.t()) :: :ok
  def insert(key, value, %Config{} = config) do
    expires_at = DateTime.add(DateTime.utc_now(), config.ttl, :millisecond)
    :ets.insert(config.cache_name, {key, {value, expires_at}})
    # The args for the warmer default to the key used for lookup
    # If you want to customize your warmer to include passed-in args,
    # just modify this to accept both an MF tuple (current) and an MFA tuple
    {mod, fun} = config.warmer
    # Proactively warm the cache so that the next lookup call doesn't miss
    :timer.apply_after(config.warm_after, mod, fun, [key])
    :ok
  end

  @spec delete(any(), Config.t()) :: :ok
  def delete(key, %Config{} = config) do
    :ets.delete(config.cache_name, key)
    :ok
  end
end
