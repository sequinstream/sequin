defmodule Sequin.Metrics.StoreTest do
  use Sequin.Case, async: true

  alias Sequin.Factory
  alias Sequin.Metrics.Store

  describe "count" do
    setup do
      [key: Factory.uuid()]
    end

    test "incr_count", ctx do
      assert Store.incr_count(ctx.key) == :ok
      assert Store.get_count(ctx.key) == {:ok, 1}
    end
  end

  describe "throughput" do
    setup do
      [key: Factory.uuid()]
    end

    test "incr_throughput", ctx do
      assert Store.incr_throughput(ctx.key) == :ok
      assert Store.incr_throughput(ctx.key) == :ok
      assert Store.incr_throughput(ctx.key) == :ok
      assert Store.incr_throughput(ctx.key) == :ok
      assert Store.incr_throughput(ctx.key) == :ok

      # We have a 5 second minimum window so 5 requests in 5 seconds is 1 request per second
      assert {:ok, throughput} = Store.get_throughput(ctx.key)
      assert Float.round(throughput, 3) == 1.0

      assert Store.incr_throughput(ctx.key) == :ok
      assert Store.incr_throughput(ctx.key) == :ok
      assert Store.incr_throughput(ctx.key) == :ok
      assert Store.incr_throughput(ctx.key) == :ok
      assert Store.incr_throughput(ctx.key) == :ok

      assert {:ok, throughput} = Store.get_throughput(ctx.key)
      assert Float.round(throughput, 3) == 2.0
    end
  end

  describe "latency" do
    setup do
      [key: Factory.uuid()]
    end

    test "incr_latency and get_latency", ctx do
      assert Store.incr_latency(ctx.key, 10.5) == :ok
      assert Store.incr_latency(ctx.key, 20.5) == :ok
      assert Store.incr_latency(ctx.key, 30.5) == :ok

      assert {:ok, avg} = Store.get_latency(ctx.key)
      # Average of 10.5, 20.5, and 30.5
      assert_in_delta avg, 20.5, 0.1

      # Test empty window
      key = Factory.uuid()
      assert {:ok, nil} = Store.get_latency(key)
    end
  end
end
