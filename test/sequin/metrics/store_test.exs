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

      Process.sleep(500)

      assert {:ok, throughput} = Store.get_throughput(ctx.key)
      assert throughput > 5.0
      assert throughput < 10.0

      # Sleep for 2 seconds to let the throughput decrease
      Process.sleep(2000)

      assert {:ok, throughput} = Store.get_throughput(ctx.key)
      assert throughput < 5.0
      assert throughput > 0.0
    end
  end
end
