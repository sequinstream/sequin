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

      updated_db = %{db | database: Factory.postgres_object()}

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
