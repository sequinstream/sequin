defmodule Sequin.TransformsTest do
  use Sequin.DataCase

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Transforms

  describe "to_external/1" do
    test "returns a map of the account" do
      account = AccountsFactory.account()
      assert %{name: name} = Transforms.to_external(account)
      assert name == account.name
    end

    test "returns a map of the user" do
      user = AccountsFactory.user()
      assert %{email: email, password: "********"} = Transforms.to_external(user)
      assert email == user.email
    end

    test "returns a map of the postgres database" do
      database = DatabasesFactory.insert_postgres_database!(table_count: 1)
      [table] = database.tables

      _replication_slot =
        ReplicationFactory.insert_postgres_replication!(
          postgres_database_id: database.id,
          account_id: database.account_id
        )

      sequence =
        DatabasesFactory.insert_sequence!(
          account_id: database.account_id,
          postgres_database_id: database.id,
          table_oid: table.oid
        )

      json = Transforms.to_external(database)

      assert %{
               id: id,
               name: name,
               username: username,
               password: "********",
               hostname: hostname,
               database: database_name,
               slot_name: slot_name,
               publication_name: publication_name,
               port: port,
               pool_size: pool_size,
               ssl: ssl,
               ipv6: ipv6,
               use_local_tunnel: use_local_tunnel,
               tables: [table_json]
             } = json

      assert id == database.id
      assert name == database.name
      assert username == database.username
      assert hostname == database.hostname
      assert database_name == database.database
      assert port == database.port
      assert pool_size == database.pool_size
      assert is_boolean(ssl)
      assert is_boolean(ipv6)
      assert is_boolean(use_local_tunnel)
      assert is_binary(slot_name)
      assert is_binary(publication_name)
      assert is_map(table_json)
      assert table_json.table_schema == table.schema
      assert table_json.table_name == table.name
      assert table_json.sort_column_name == sequence.sort_column_name
    end

    test "returns a map of the column filter" do
      column_filter = ConsumersFactory.column_filter()
      json = Transforms.to_external(column_filter)

      assert %{
               column_name: column_name,
               operator: operator,
               comparison_value: value
             } = json

      assert column_name == column_filter.column_name
      assert operator == column_filter.operator
      assert value == column_filter.value.value
    end

    test "returns a map of the wal pipeline" do
      account = AccountsFactory.insert_account!()

      source_db = DatabasesFactory.insert_postgres_database!(account_id: account.id, table_count: 1)
      [source_table] = source_db.tables
      [column | _] = source_table.columns

      ReplicationFactory.insert_postgres_replication!(postgres_database_id: source_db.id, account_id: account.id)

      source_db = Repo.preload(source_db, [:replication_slot])

      dest_db = DatabasesFactory.insert_postgres_database!(account_id: account.id, table_count: 1)
      [dest_table] = dest_db.tables

      column_filter = ConsumersFactory.column_filter(column_attnum: column.attnum)
      source_table = ConsumersFactory.source_table(oid: source_table.oid, column_filters: [column_filter])

      wal_pipeline =
        ReplicationFactory.wal_pipeline(
          replication_slot_id: source_db.replication_slot.id,
          destination_database_id: dest_db.id,
          destination_oid: dest_table.oid,
          source_tables: [source_table]
        )

      json = Transforms.to_external(wal_pipeline)

      assert %{
               id: id,
               name: name,
               source_database: source_database,
               source_table_schema: _source_table_schema,
               source_table_name: _source_table_name,
               destination_database: destination_database,
               destination_table_schema: _destination_table_schema,
               destination_table_name: _destination_table_name,
               filters: filters,
               actions: actions
             } = json

      assert id == wal_pipeline.id
      assert name == wal_pipeline.name
      assert source_database == source_db.name
      assert destination_database == dest_db.name
      assert is_list(filters)

      Enum.each(filters, fn filter ->
        assert %{column_name: column_name, operator: operator, comparison_value: value} = filter
        assert column_name == column.name
        assert operator == column_filter.operator
        assert value == column_filter.value.value
      end)

      assert is_list(actions)
    end

    test "returns a map of webhook.site endpoint" do
      endpoint =
        ConsumersFactory.http_endpoint(
          name: "webhook_endpoint",
          host: "webhook.site"
        )

      json = Transforms.to_external(endpoint)

      assert %{
               name: "webhook_endpoint",
               "webhook.site": true
             } = json
    end

    test "returns a map of local tunnel endpoint" do
      endpoint =
        ConsumersFactory.http_endpoint(
          name: "local_endpoint",
          use_local_tunnel: true,
          path: "/webhook",
          headers: %{"Content-Type" => "application/json"},
          encrypted_headers: %{"Authorization" => "secret"}
        )

      json = Transforms.to_external(endpoint)

      assert %{
               name: "local_endpoint",
               local: true,
               path: "/webhook",
               headers: %{"Content-Type" => "application/json"},
               encrypted_headers: "(1 encrypted header(s)) - sha256sum: " <> _
             } = json
    end

    test "returns a map of standard http endpoint" do
      endpoint =
        ConsumersFactory.http_endpoint(
          name: "standard_endpoint",
          scheme: :https,
          host: "api.example.com",
          port: 443,
          path: "/webhook",
          headers: %{"Content-Type" => "application/json"},
          encrypted_headers: %{"Authorization" => "secret"}
        )

      json = Transforms.to_external(endpoint)

      assert %{
               name: "standard_endpoint",
               url: "https://api.example.com/webhook",
               headers: %{"Content-Type" => "application/json"},
               encrypted_headers: "(1 encrypted header(s)) - sha256sum: " <> _
             } = json
    end
  end

  test "returns a map of the http push consumer" do
    account = AccountsFactory.insert_account!()
    database = DatabasesFactory.insert_postgres_database!(account_id: account.id, table_count: 1)
    [table] = database.tables
    [column | _] = table.columns

    sequence =
      DatabasesFactory.insert_sequence!(
        account_id: account.id,
        postgres_database_id: database.id,
        table_oid: table.oid
      )

    endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id, name: "test-endpoint")

    consumer =
      ConsumersFactory.insert_sink_consumer!(
        name: "test-consumer",
        account_id: account.id,
        status: :active,
        max_ack_pending: 1000,
        max_deliver: 5,
        sink: %{type: :http_push, http_endpoint_id: endpoint.id},
        sequence_id: sequence.id,
        sequence_filter: %{
          group_column_attnums: [column.attnum],
          actions: [:insert, :update],
          column_filters: [
            ConsumersFactory.sequence_filter_column_filter_attrs(
              column_attnum: column.attnum,
              operator: :==,
              value: %{__type__: :string, value: "test"}
            )
          ]
        }
      )

    consumer = %{consumer | sink: %{consumer.sink | http_endpoint: endpoint}}
    json = Transforms.to_external(consumer)

    assert %{
             name: name,
             status: status,
             max_deliver: max_deliver,
             database: database_name,
             table: schema_and_table,
             destination: %{
               type: "webhook",
               http_endpoint: endpoint_name
             },
             consumer_start: %{
               position: "beginning | end | from with value"
             },
             group_column_names: group_column_names,
             filters: filters
           } = json

    assert name == "test-consumer"
    assert endpoint_name == "test-endpoint"
    assert database_name == database.name
    assert schema_and_table == "#{table.schema}.#{table.name}"
    assert status == :active
    assert max_deliver == 5
    assert group_column_names == [column.name]
    assert length(filters) == 1
    [filter] = filters

    assert %{
             column_name: _,
             operator: "==",
             comparison_value: "test"
           } = filter
  end
end
