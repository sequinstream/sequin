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

  describe "average" do
    setup do
      [key: Factory.uuid()]
    end

    test "incr_avg", ctx do
      assert Store.incr_avg(ctx.key, 10) == :ok
      assert Store.get_avg(ctx.key) == {:ok, 10}

      assert Store.incr_avg(ctx.key, 20) == :ok
      assert Store.get_avg(ctx.key) == {:ok, 15}
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

      # We have a 60 second minimum window so 5 requests in 60 seconds is 0.0833 requests per second
      assert {:ok, throughput} = Store.get_throughput(ctx.key)
      assert Float.round(throughput, 3) == 0.083

      assert Store.incr_throughput(ctx.key) == :ok
      assert Store.incr_throughput(ctx.key) == :ok
      assert Store.incr_throughput(ctx.key) == :ok
      assert Store.incr_throughput(ctx.key) == :ok
      assert Store.incr_throughput(ctx.key) == :ok

      assert {:ok, throughput} = Store.get_throughput(ctx.key)
      assert Float.round(throughput, 3) == 0.167
    end
  end
end
