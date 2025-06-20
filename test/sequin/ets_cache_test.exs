defmodule Sequin.EtsCacheTest do
  use Sequin.Case, async: true

  alias Sequin.EtsCache

  @cache :cache_for_test

  defmodule A do
    @moduledoc false
    def warmer(key) do
      EtsCache.insert(key, :valid, Sequin.EtsCacheTest.config())
    end
  end

  describe "EtsCache" do
    test "cache miss calls function, loads result into cache" do
      key = UUID.uuid4()
      # effectively disable warming
      c = config(%{ttl: :timer.seconds(1), warm_after: :timer.seconds(10)})

      fun = fn ->
        send(self(), "lookup")
        :valid
      end

      :valid = EtsCache.lookup(key, c, fun)
      assert_received "lookup"
      :valid = EtsCache.lookup(key, c, fun)
      refute_received "lookup"
      Process.sleep(c.ttl + 100)
      :valid = EtsCache.lookup(key, c, fun)
      assert_received "lookup"
      :valid = EtsCache.lookup(key, c, fun)
      refute_received "lookup"
    end

    test "cache is warmed proactively" do
      key = UUID.uuid4()
      c = config()

      fun = fn ->
        send(self(), "lookup")
        :valid
      end

      :valid = EtsCache.lookup(key, c, fun)
      assert_received "lookup"
      :valid = EtsCache.lookup(key, c, fun)
      refute_received "lookup"
      Process.sleep(c.warm_after + 100)
      # Ensure the warming function gets a chance to run
      :erlang.yield()
      :valid = EtsCache.lookup(key, c, fun)
      refute_received "lookup"
      Process.sleep(c.ttl + 100)
      :valid = EtsCache.lookup(key, c, fun)
      refute_received "lookup"
    end
  end

  def config(attrs \\ %{}) do
    ttl = Map.get(attrs, :ttl, :timer.seconds(2))
    warm_after = Map.get(attrs, :warm_after, :timer.seconds(1))

    %EtsCache.Config{
      cache_name: @cache,
      ttl: ttl,
      warm_after: warm_after,
      warmer: {A, :warmer}
    }
  end
end
