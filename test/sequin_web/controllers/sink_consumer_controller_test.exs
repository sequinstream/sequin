defmodule SequinWeb.SinkConsumerControllerTest do
  use SequinWeb.ConnCase, async: true

  alias Sequin.Consumers
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.FunctionsFactory
  alias Sequin.Factory.ReplicationFactory

  setup :authenticated_conn

  setup %{account: account} do
    # Create one provisioned sink consumer
    sink_consumer = insert_provisioned_consumer(account)

    other_account = AccountsFactory.insert_account!()
    other_sink_consumer = insert_provisioned_consumer(other_account)

    # Create the resources necessary for other consumer creates, including database with table and sequence
    column_attrs = DatabasesFactory.column_attrs(type: "text", is_pk?: true)
    table_attrs = DatabasesFactory.table_attrs(columns: [column_attrs])
    database = DatabasesFactory.insert_postgres_database!(account_id: account.id, tables: [table_attrs])
    [table] = database.tables
    column = List.first(table.columns)

    ReplicationFactory.insert_postgres_replication!(account_id: account.id, postgres_database_id: database.id)

    http_endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id)

    valid_attrs = %{
      name: "inventore_6730",
      status: "active",
      destination: %{
        type: "webhook",
        http_endpoint: http_endpoint.name
      },
      database: database.name,
      transform: "none",
      batch_size: 1,
      actions: ["insert", "update", "delete"],
      tables: [
        %{name: "#{table.schema}.#{table.name}", group_column_names: [column.name]}
      ],
      source: %{
        include_tables: ["#{table.schema}.#{table.name}"]
      },
      max_retry_count: 5,
      load_shedding_policy: "pause_on_full",
      annotations: %{
        user_name: "Thingy"
      }
    }

    %{
      database: database,
      table: table,
      column: column,
      http_endpoint: http_endpoint,
      sink_consumer: sink_consumer,
      other_sink_consumer: other_sink_consumer,
      other_account: other_account,
      valid_attrs: valid_attrs
    }
  end

  describe "index" do
    test "lists sink consumers in the given account", %{
      conn: conn,
      account: account,
      sink_consumer: sink_consumer
    } do
      another_consumer = insert_provisioned_consumer(account)

      conn = get(conn, ~p"/api/sinks")
      assert %{"data" => consumers} = json_response(conn, 200)
      assert length(consumers) == 2
      atomized_consumers = Enum.map(consumers, &Sequin.Map.atomize_keys/1)
      assert_lists_equal([sink_consumer, another_consumer], atomized_consumers, &(&1.name == &2.name))
      assert Enum.all?(atomized_consumers, & &1.health)
    end

    test "does not list sink consumers from another account", %{
      conn: conn,
      other_sink_consumer: other_sink_consumer
    } do
      conn = get(conn, ~p"/api/sinks")
      assert %{"data" => consumers} = json_response(conn, 200)
      refute Enum.any?(consumers, &(&1["id"] == other_sink_consumer.id))
    end
  end

  describe "show" do
    test "shows sink consumer details", %{conn: conn, sink_consumer: sink_consumer} do
      conn = get(conn, ~p"/api/sinks/#{sink_consumer.id}")
      assert sink_consumer_json = json_response(conn, 200)

      assert sink_consumer.name == sink_consumer_json["name"]
      assert is_map(sink_consumer_json["health"])
    end

    test "returns 404 if sink consumer belongs to another account", %{
      conn: conn,
      other_sink_consumer: other_sink_consumer
    } do
      conn = get(conn, ~p"/api/sinks/#{other_sink_consumer.id}")
      assert json_response(conn, 404)
    end
  end

  describe "create" do
    test "creates a webhook sink consumer with valid attributes", %{
      conn: conn,
      account: account,
      valid_attrs: valid_attrs,
      table: table
    } do
      conn = post(conn, ~p"/api/sinks", valid_attrs)
      assert %{"name" => name} = json_response(conn, 200)

      {:ok, sink_consumer} = Consumers.find_sink_consumer(account.id, name: name)
      assert sink_consumer.account_id == account.id
      assert sink_consumer.name == valid_attrs.name
      assert sink_consumer.status == String.to_atom(valid_attrs.status)

      assert sink_consumer.sink.type == :http_push
      assert sink_consumer.actions == Enum.map(valid_attrs.actions, &String.to_atom/1)
      assert is_nil(sink_consumer.transform_id)
      assert [table_oid] = sink_consumer.source.include_table_oids
      assert table_oid == table.oid
      assert sink_consumer.batch_size == valid_attrs.batch_size
      assert sink_consumer.max_retry_count == valid_attrs.max_retry_count
      assert sink_consumer.load_shedding_policy == String.to_atom(valid_attrs.load_shedding_policy)
      assert %{"user_name" => "Thingy"} = sink_consumer.annotations
    end

    test "creates a webhook sink consumer defaulting batch to true", %{
      conn: conn,
      account: account,
      valid_attrs: valid_attrs
    } do
      conn = post(conn, ~p"/api/sinks", valid_attrs)
      assert %{"name" => name} = json_response(conn, 200)

      {:ok, sink_consumer} = Consumers.find_sink_consumer(account.id, name: name)
      assert sink_consumer.sink.batch == true
    end

    test "creates a webhook sink consumer with batch set to false", %{
      conn: conn,
      account: account,
      valid_attrs: valid_attrs
    } do
      valid_attrs = put_in(valid_attrs, [:destination, :batch], false)
      conn = post(conn, ~p"/api/sinks", valid_attrs)
      assert %{"name" => name} = json_response(conn, 200)

      {:ok, sink_consumer} = Consumers.find_sink_consumer(account.id, name: name)
      assert sink_consumer.sink.batch == false
    end

    test "can't create a webhook sink consumer with batch set to false and batch size set to > 1", %{
      conn: conn,
      valid_attrs: valid_attrs
    } do
      valid_attrs =
        valid_attrs
        |> put_in([:destination, :batch], false)
        |> Map.put(:batch_size, 100)

      conn = post(conn, ~p"/api/sinks", valid_attrs)
      assert json_response(conn, 422)
    end

    test "can't create a webhook sink consumer referencing a table not in the database", %{
      conn: conn,
      valid_attrs: valid_attrs
    } do
      valid_attrs = %{valid_attrs | tables: [%{name: "non_existent_table"}]}

      conn = post(conn, ~p"/api/sinks", valid_attrs)
      assert res = json_response(conn, 422)
      assert res["summary"] =~ "non_existent_table"
    end

    test "returns validation error for invalid attributes", %{conn: conn} do
      invalid_attrs = %{name: nil}
      conn = post(conn, ~p"/api/sinks", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "ignores provided account_id and uses authenticated account", %{
      conn: conn,
      account: account,
      other_account: other_account,
      database: database
    } do
      http_endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id)

      # Use a similar hardcoded map, potentially adjust values if needed for this specific test case
      create_attrs = %{
        # Slightly different name for clarity if desired
        name: "inventore_6731",
        status: "active",
        destination: %{
          type: "webhook",
          http_endpoint: http_endpoint.name
        },
        database: database.name
      }

      # Add the incorrect account_id to the hardcoded map
      attrs_with_wrong_account = Map.put(create_attrs, :account_id, other_account.id)

      conn = post(conn, ~p"/api/sinks", attrs_with_wrong_account)

      assert %{"id" => id} = json_response(conn, 200)

      {:ok, sink_consumer} = Consumers.find_sink_consumer(account.id, id: id)
      assert sink_consumer.account_id == account.id
      assert sink_consumer.account_id != other_account.id
    end
  end

  describe "update" do
    test "updates the sink consumer with valid attributes", %{conn: conn, sink_consumer: sink_consumer} do
      name = sink_consumer.name
      {:ok, _} = Consumers.update_sink_consumer(sink_consumer, %{name: "some-old-name"})
      update_attrs = %{name: name}
      conn = put(conn, ~p"/api/sinks/#{sink_consumer.id}", update_attrs)
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, updated_consumer} = Consumers.find_sink_consumer(sink_consumer.account_id, id: id)
      assert updated_consumer.name == name

      assert updated_consumer.sink == sink_consumer.sink
      assert updated_consumer.transform == sink_consumer.transform
      assert updated_consumer.batch_size == sink_consumer.batch_size
    end

    test "updates the sink consumer with advance config", %{conn: conn, sink_consumer: sink_consumer} do
      update_attrs = %{max_retry_count: 5, load_shedding_policy: "discard_on_full"}
      conn = put(conn, ~p"/api/sinks/#{sink_consumer.id}", update_attrs)
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, updated_consumer} = Consumers.find_sink_consumer(sink_consumer.account_id, id: id)
      assert updated_consumer.max_retry_count == 5
      assert updated_consumer.load_shedding_policy == :discard_on_full
    end

    test "updates the sink consumer to set batch=false", %{
      conn: conn,
      sink_consumer: sink_consumer,
      valid_attrs: valid_attrs
    } do
      conn =
        put(conn, ~p"/api/sinks/#{sink_consumer.id}", %{destination: Map.put(valid_attrs.destination, :batch, false)})

      assert %{"id" => id} = json_response(conn, 200)

      {:ok, updated_consumer} = Consumers.find_sink_consumer(sink_consumer.account_id, id: id)
      assert updated_consumer.sink.batch == false
    end

    test "cannot update a sink consumer to set batch=false if batch_size > 1", %{
      conn: conn,
      sink_consumer: sink_consumer,
      valid_attrs: valid_attrs
    } do
      Consumers.update_sink_consumer(sink_consumer, %{batch_size: 100})

      conn =
        put(conn, ~p"/api/sinks/#{sink_consumer.id}", %{destination: Map.put(valid_attrs.destination, :batch, false)})

      assert json_response(conn, 422)
    end

    test "returns validation error for invalid attributes", %{conn: conn, sink_consumer: sink_consumer} do
      invalid_attrs = %{status: "invalid"}
      conn = put(conn, ~p"/api/sinks/#{sink_consumer.id}", invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end

    test "returns 404 if sink consumer belongs to another account", %{
      conn: conn,
      other_sink_consumer: other_sink_consumer
    } do
      conn = put(conn, ~p"/api/sinks/#{other_sink_consumer.id}", %{name: "new-name"})
      assert json_response(conn, 404)
    end

    test "can't update a webhook sink consumer referencing a table not in the database", %{
      conn: conn,
      sink_consumer: sink_consumer,
      valid_attrs: valid_attrs
    } do
      valid_attrs = %{valid_attrs | source: %{include_tables: ["non_existent_table"]}}

      conn = put(conn, ~p"/api/sinks/#{sink_consumer.id}", valid_attrs)
      assert res = json_response(conn, 422)
      assert res["summary"] =~ "non_existent_table"
    end

    test "updating a sink consumer with functions does not remove those functions", %{
      conn: conn,
      sink_consumer: sink_consumer
    } do
      transform = FunctionsFactory.insert_transform_function!(account_id: sink_consumer.account_id)
      filter = FunctionsFactory.insert_filter_function!(account_id: sink_consumer.account_id)
      Consumers.update_sink_consumer(sink_consumer, %{transform_id: transform.id, filter_id: filter.id})

      conn = put(conn, ~p"/api/sinks/#{sink_consumer.id}", %{name: "new-name"})
      assert %{"id" => id} = json_response(conn, 200)

      {:ok, updated_consumer} = Consumers.find_sink_consumer(sink_consumer.account_id, id: id)
      assert updated_consumer.transform_id == transform.id
      assert updated_consumer.filter_id == filter.id
      assert updated_consumer.name == "new-name"
    end
  end

  describe "delete" do
    test "deletes the sink consumer", %{conn: conn, sink_consumer: sink_consumer} do
      conn = delete(conn, ~p"/api/sinks/#{sink_consumer.id}")
      assert %{"id" => id, "deleted" => true} = json_response(conn, 200)

      assert {:error, _} = Consumers.find_sink_consumer(sink_consumer.account_id, id: id)
    end

    test "returns 404 if sink consumer belongs to another account", %{
      conn: conn,
      other_sink_consumer: other_sink_consumer
    } do
      conn = delete(conn, ~p"/api/sinks/#{other_sink_consumer.id}")
      assert json_response(conn, 404)
    end
  end

  defp insert_provisioned_consumer(account) do
    column_attrs = DatabasesFactory.column_attrs(type: "text")
    table_attrs = DatabasesFactory.table_attrs(columns: [column_attrs])
    database = DatabasesFactory.insert_postgres_database!(account_id: account.id, tables: [table_attrs])

    replication =
      ReplicationFactory.insert_postgres_replication!(account_id: account.id, postgres_database_id: database.id)

    ConsumersFactory.insert_sink_consumer!(
      account_id: account.id,
      postgres_database: database,
      replication_slot_id: replication.id
    )
  end
end
