defmodule Sequin.Extensions.ReplicationTest do
  use Sequin.DataCase, async: true

  alias Sequin.Extensions.PostgresAdapter.Changes.NewRecord
  alias Sequin.Extensions.Replication
  alias Sequin.Repo

  @test_schema "__test_cdc__"
  @test_table "test_table"
  @publication "test_publication"
  @slot_name "pg_cdc_test"

  setup_all do
    config =
      :sequin
      |> Application.get_env(Sequin.Repo)
      |> Keyword.take([:username, :password, :hostname, :database, :port])

    # These tests need to happen outside of the sandbox
    {:ok, conn} = Postgrex.start_link(config)

    query(conn, """
    select pg_drop_replication_slot('#{@slot_name}');
    """)

    query!(conn, """
    create schema if not exists #{@test_schema};
    """)

    query!(conn, """
    drop table if exists #{@test_schema}.#{@test_table};
    """)

    query!(conn, """
    create table #{@test_schema}.#{@test_table} (
      id serial primary key,
      first_name text,
      last_name text
    );
    """)

    query!(
      conn,
      """
      CREATE PUBLICATION #{@publication} FOR TABLE #{@test_schema}.#{@test_table};
      """,
      [],
      ignore_error_code: :duplicate_object
    )

    on_exit(fn ->
      query(conn, """
      drop publication if exists #{@publication};
      """)
    end)

    [conn: conn]
  end

  @server_id __MODULE__
  @server_via Replication.via_tuple(@server_id)
  describe "handling changes from replication slot" do
    test "changes are buffered, even if the listener is not up", %{conn: conn} do
      start_replication!(handle_message_fun: fn _ -> :ok end)

      assert_receive {Replication, :slot_created}

      stop_replication!()

      query!(conn, "insert into #{@test_schema}.#{@test_table} (first_name, last_name) values ('Paul', 'Atreides')")

      test_pid = self()
      start_replication!(handle_message_fun: fn msg -> send(test_pid, {:change, msg}) end)

      assert_receive {:change, change}, :timer.seconds(1)

      assert is_struct(change, NewRecord)

      assert change.record == %{
               "id" => "1",
               "first_name" => "Paul",
               "last_name" => "Atreides"
             }

      assert change.table == @test_table
      assert change.schema == @test_schema
    end

    # test "changes are delivered at least once"
    # test "creates are captured"
    # test "updates are captured"
    # test "deletes are captured"
  end

  defp start_replication!(opts) do
    opts =
      Keyword.merge(
        [publication: @publication, connection: Repo.config(), slot_name: @slot_name, test_pid: self(), id: @server_id],
        opts
      )

    start_supervised!(Replication.child_spec(opts))
  end

  defp stop_replication! do
    stop_supervised!(@server_via)
  end

  # describe "changes propagating to a stream" do
  # setup - an actual replication object. Boot the replication supervisor?
  # - creates are inserted
  # - updates are upserted
  # - deletes propagate as upserts
  # end

  defp query!(conn, query, params \\ [], opts \\ []) do
    case query(conn, query, params, opts) do
      :ok -> :ok
      error -> raise error
    end
  end

  defp query(conn, query, params \\ [], opts \\ []) do
    {ignore_error_code, opts} = Keyword.pop(opts, :ignore_error_code, [])

    case Postgrex.query(conn, query, params, opts) do
      {:ok, _res} -> :ok
      {:error, %{postgres: %{code: ^ignore_error_code}}} -> :ok
      error -> error
    end
  end
end
