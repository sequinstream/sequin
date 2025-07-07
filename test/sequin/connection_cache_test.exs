defmodule Sequin.Databases.ConnectionCacheTest do
  use Sequin.Case, async: true

  alias Sequin.Databases.ConnectionCache
  alias Sequin.Factory
  alias Sequin.Factory.DatabasesFactory

  describe "connection caching" do
    test "starts a connection if not cached" do
      db = DatabasesFactory.postgres_database()
      cache = start_cache()

      assert {:ok, pid} = ConnectionCache.connection(cache, db)
      assert Process.alive?(pid)
    end

    test "returns existing connection when cached" do
      db = DatabasesFactory.postgres_database()
      cache = start_cache()

      {:ok, pid} = ConnectionCache.connection(cache, db)
      assert {:ok, ^pid} = ConnectionCache.connection(cache, db)
    end

    test "works with a configured database" do
      db = DatabasesFactory.postgres_database(DatabasesFactory.configured_postgres_database_attrs())
      cache = start_cache()

      assert {:ok, pid} = ConnectionCache.connection(cache, db)
      assert Process.alive?(pid)
      assert {:ok, ^pid} = ConnectionCache.connection(cache, db)
    end

    test "stops all connections when terminated" do
      db = DatabasesFactory.postgres_database()
      cache = start_cache()

      {:ok, pid} = ConnectionCache.connection(cache, db)

      stop_supervised!(ConnectionCache)

      refute Process.alive?(pid)
    end

    test "starts connections for different databases" do
      db1 = DatabasesFactory.postgres_database()
      db2 = DatabasesFactory.postgres_database()
      cache = start_cache()

      {:ok, pid1} = ConnectionCache.connection(cache, db1)
      {:ok, pid2} = ConnectionCache.connection(cache, db2)

      refute pid1 == pid2
    end

    test "returns error when connection can't be started" do
      db = DatabasesFactory.postgres_database()
      error = {:error, %Postgrex.Error{message: "Failed to start"}}
      cache = start_cache(start_fn: fn ^db -> error end)

      assert ^error = ConnectionCache.connection(cache, db)
    end

    test "starts a new connection and closes old one if connection options have changed" do
      db = DatabasesFactory.postgres_database()
      cache = start_cache()

      {:ok, pid} = ConnectionCache.connection(cache, db)

      updated_db = %{db | database: Factory.postgres_object() <> "_changed"}

      {:ok, new_pid} = ConnectionCache.connection(cache, updated_db)

      refute pid == new_pid
      assert Process.alive?(new_pid)
      refute Process.alive?(pid)
      assert {:ok, ^new_pid} = ConnectionCache.connection(cache, updated_db)
    end

    @tag capture_log: true
    test "starts a new connection if old one is dead" do
      db = DatabasesFactory.postgres_database()
      cache = start_cache()

      {:ok, pid} = ConnectionCache.connection(cache, db)

      Process.exit(pid, :kill)

      {:ok, new_pid} = ConnectionCache.connection(cache, db)

      refute new_pid == pid
      assert Process.alive?(new_pid)
    end

    test "returns error when connection is not found and create_on_miss is false" do
      db = DatabasesFactory.postgres_database()
      cache = start_cache()

      assert {:error, %Sequin.Error.NotFoundError{}} = ConnectionCache.existing_connection(cache, db)
    end

    test "when multiple callers want to start the same connection at the same time, fans out the result to them" do
      test_pid = self()

      db = DatabasesFactory.postgres_database()

      start_fn = fn _db ->
        send(test_pid, {:started, self()})

        receive do
          :go -> {:ok, spawn(fn -> Process.sleep(5_000) end)}
        after
          1_000 -> raise "Timed out waiting for go signal"
        end
      end

      cache = start_cache(start_fn: start_fn)

      task1 = Task.async(fn -> ConnectionCache.connection(cache, db) end)
      assert_receive {:started, pid}
      task2 = Task.async(fn -> ConnectionCache.connection(cache, db) end)
      :erlang.yield()

      send(pid, :go)

      res = Task.await_many([task1, task2])
      [{:ok, pid1}, {:ok, pid1}] = res

      refute_received {:started, _pid}
    end

    test "if start_fn fails, returns the error to the callers" do
      test_pid = self()
      db = DatabasesFactory.postgres_database()
      expected_error = %Postgrex.Error{message: "Failed to start"}

      start_fn = fn _db ->
        send(test_pid, {:started, self()})

        receive do
          :go -> {:error, expected_error}
        after
          5_000 -> raise "Timed out waiting for go signal"
        end
      end

      cache = start_cache(start_fn: start_fn)

      task1 = Task.async(fn -> ConnectionCache.connection(cache, db) end)
      assert_receive {:started, pid}
      task2 = Task.async(fn -> ConnectionCache.connection(cache, db) end)

      Process.sleep(1)
      :erlang.yield()

      send(pid, :go)

      res = Task.await_many([task1, task2])
      assert length(res) == 2
      assert [{:error, ^expected_error}, {:error, ^expected_error}] = res
    end

    defp start_cache(overrides \\ []) do
      opts =
        Keyword.merge(
          [
            name: Module.concat(__MODULE__, ConnectionCache),
            start_fn: fn _db -> {:ok, spawn(fn -> Process.sleep(5_000) end)} end,
            stop_fn: &Process.exit(&1, :kill)
          ],
          overrides
        )

      start_supervised!({ConnectionCache, opts})
    end
  end
end
