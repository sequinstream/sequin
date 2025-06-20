defmodule Sequin.TransformsTest do
  use Sequin.DataCase

  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.FunctionsFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.ConsumerLifecycleEventWorker, as: CLEW
  alias Sequin.Transforms
  alias Sequin.Transforms.SensitiveValue

  describe "to_external/1" do
    test "returns a map of the account" do
      account = AccountsFactory.account()
      assert %{name: name} = Transforms.to_external(account)
      assert name == account.name
    end

    test "returns a map of the user" do
      user = AccountsFactory.user()
      assert %{email: email, password: password} = Transforms.to_external(user)
      assert email == user.email

      assert password == %SensitiveValue{
               value: user.password,
               show_value: false
             }
    end

    test "returns a map of the postgres database" do
      database = DatabasesFactory.insert_postgres_database!(table_count: 1)

      _replication_slot =
        ReplicationFactory.insert_postgres_replication!(
          postgres_database_id: database.id,
          account_id: database.account_id
        )

      json = Transforms.to_external(database)

      assert %{
               id: id,
               name: name,
               username: username,
               password: password,
               hostname: hostname,
               database: database_name,
               slot: %{name: slot_name},
               publication: %{name: publication_name},
               port: port,
               pool_size: pool_size,
               ssl: ssl,
               ipv6: ipv6,
               use_local_tunnel: use_local_tunnel
             } = json

      assert id == database.id
      assert name == database.name
      assert username == database.username
      assert password == %Sequin.Transforms.SensitiveValue{value: database.password, show_value: false}
      assert hostname == database.hostname
      assert database_name == database.database
      assert port == database.port
      assert pool_size == database.pool_size
      assert is_boolean(ssl)
      assert is_boolean(ipv6)
      assert is_boolean(use_local_tunnel)
      assert is_binary(slot_name)
      assert is_binary(publication_name)
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

      column_filter = ReplicationFactory.column_filter(column_attnum: column.attnum)
      source_table = ReplicationFactory.source_table(oid: source_table.oid, column_filters: [column_filter])

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
               encrypted_headers: %{
                 "Authorization" => %Sequin.Transforms.SensitiveValue{
                   value: "secret",
                   show_value: false
                 }
               }
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
               encrypted_headers: %{
                 "Authorization" => %Sequin.Transforms.SensitiveValue{
                   value: "secret",
                   show_value: false
                 }
               }
             } = json
    end

    test "returns a map of a sink consumer with a source" do
      consumer = ConsumersFactory.sink_consumer(source: ConsumersFactory.source())
      json = Transforms.to_external(consumer)

      assert %{include_schemas: _, exclude_schemas: _, include_tables: _, exclude_tables: _} = json.source
    end

    test "returns a map of the gcp pubsub consumer" do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id, table_count: 1)
      [table] = database.tables
      [column | _] = table.columns

      credentials = %{
        "type" => "service_account",
        "project_id" => "my-project",
        "private_key_id" => "key123",
        "private_key" => "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----\n",
        "client_email" => "my-service-account@my-project.iam.gserviceaccount.com",
        "client_id" => "123456789",
        "auth_uri" => "https://accounts.google.com/o/oauth2/auth",
        "token_uri" => "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url" => "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url" =>
          "https://www.googleapis.com/robot/v1/metadata/x509/my-service-account%40my-project.iam.gserviceaccount.com"
      }

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "pubsub-consumer",
          account_id: account.id,
          postgres_database_id: database.id,
          status: :active,
          sink: %{
            type: :gcp_pubsub,
            project_id: "my-project",
            topic_id: "my-topic",
            credentials: credentials
          },
          source_tables: [
            ConsumersFactory.source_table_attrs(table_oid: table.oid, group_column_attnums: [column.attnum])
          ]
        )

      json = Transforms.to_external(consumer)

      assert %{
               name: name,
               status: status,
               database: database_name,
               tables: source_tables,
               destination: %{
                 type: "gcp_pubsub",
                 project_id: project_id,
                 topic_id: topic_id,
                 credentials: %{
                   #  api_key: %SensitiveValue{value: nil, show_value: false},
                   auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs",
                   auth_uri: "https://accounts.google.com/o/oauth2/auth",
                   client_email: %SensitiveValue{
                     value: "my-service-account@my-project.iam.gserviceaccount.com",
                     show_value: false
                   },
                   client_id: %SensitiveValue{
                     value: "123456789",
                     show_value: false
                   },
                   #  client_secret: %SensitiveValue{value: nil, show_value: false},
                   client_x509_cert_url:
                     "https://www.googleapis.com/robot/v1/metadata/x509/my-service-account%40my-project.iam.gserviceaccount.com",
                   private_key: %SensitiveValue{
                     value: "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----\n",
                     show_value: false
                   },
                   private_key_id: %SensitiveValue{
                     value: "key123",
                     show_value: false
                   },
                   project_id: "my-project",
                   token_uri: "https://oauth2.googleapis.com/token",
                   type: "service_account"
                   #  universe_domain: %SensitiveValue{value: nil, show_value: false}
                 }
               }
             } = json

      assert name == "pubsub-consumer"
      assert project_id == "my-project"
      assert topic_id == "my-topic"
      assert database_name == database.name
      assert status == :active

      assert [%{name: table_ref, group_column_names: group_column_names}] = source_tables
      assert table_ref == "#{table.schema}.#{table.name}"
      assert group_column_names == [column.name]
    end

    test "returns a map of the elasticsearch consumer" do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id, table_count: 1)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "elasticsearch-consumer",
          account_id: account.id,
          status: :active,
          postgres_database_id: database.id,
          sink: %{
            type: :elasticsearch,
            endpoint_url: "https://elasticsearch.example.com",
            index_name: "test-index",
            auth_type: :api_key,
            auth_value: "sensitive-api-key",
            batch_size: 100
          }
        )

      json = Transforms.to_external(consumer)

      assert %{
               name: name,
               status: status,
               database: database_name,
               destination: %{
                 type: "elasticsearch",
                 endpoint_url: endpoint_url,
                 index_name: index_name,
                 auth_type: auth_type,
                 auth_value: %SensitiveValue{
                   value: "sensitive-api-key",
                   show_value: false
                 },
                 batch_size: batch_size
               }
             } = json

      assert name == "elasticsearch-consumer"
      assert endpoint_url == "https://elasticsearch.example.com"
      assert index_name == "test-index"
      assert auth_type == :api_key
      assert batch_size == 100
      assert database_name == database.name
      assert status == :active
    end

    test "returns a map of the redis_string consumer" do
      account = AccountsFactory.insert_account!()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id, table_count: 1)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          name: "redis-string-consumer",
          account_id: account.id,
          status: :active,
          postgres_database_id: database.id,
          sink: %{
            type: :redis_string,
            host: "redis-string.example.com",
            port: 6379,
            database: 0,
            tls: false
          }
        )

      json = Transforms.to_external(consumer)

      assert %{
               name: name,
               status: status,
               database: database_name,
               destination: %{
                 database: database_number,
                 host: host,
                 port: port,
                 tls: tls,
                 type: "redis_string"
               }
             } = json

      assert name == "redis-string-consumer"
      assert host == "redis-string.example.com"
      assert port == 6379
      assert database_number == 0
      assert database_name == database.name
      assert tls == false
      assert status == :active
    end
  end

  test "returns a map of the http push consumer" do
    account = AccountsFactory.insert_account!()
    database = DatabasesFactory.insert_postgres_database!(account_id: account.id, table_count: 1)

    endpoint = ConsumersFactory.insert_http_endpoint!(account_id: account.id, name: "test-endpoint")

    consumer =
      ConsumersFactory.insert_sink_consumer!(
        name: "test-consumer",
        account_id: account.id,
        postgres_database_id: database.id,
        status: :active,
        max_ack_pending: 1000,
        sink: %{type: :http_push, http_endpoint_id: endpoint.id}
      )

    consumer = %{consumer | sink: %{consumer.sink | http_endpoint: endpoint}}
    json = Transforms.to_external(consumer)

    assert %{
             name: name,
             status: status,
             database: database_name,
             destination: %{
               type: "webhook",
               http_endpoint: endpoint_name
             }
           } = json

    assert name == "test-consumer"
    assert endpoint_name == "test-endpoint"
    assert database_name == database.name
    assert status == :active
  end

  describe "path_transform/1" do
    test "functions a consumer message with a top-level field path" do
      message = ConsumersFactory.consumer_message()
      path_function = FunctionsFactory.insert_path_function!(function_attrs: [path: "record"])
      consumer = %SinkConsumer{transform: path_function}

      result = Transforms.Message.to_external(consumer, message)
      assert result == message.data.record
    end

    test "functions a consumer message with a nested field path" do
      message = ConsumersFactory.consumer_message()
      path_function = FunctionsFactory.insert_path_function!(function_attrs: [path: "record.id"])
      consumer = %SinkConsumer{transform: path_function}

      result = Transforms.Message.to_external(consumer, message)
      assert result == get_in(message.data.record, ["id"])
    end

    test "functions a consumer message with metadata path" do
      message = ConsumersFactory.consumer_message()

      path_function = FunctionsFactory.insert_path_function!(function_attrs: [path: "metadata.table_schema"])

      consumer = %SinkConsumer{transform: path_function}

      result = Transforms.Message.to_external(consumer, message)
      assert result == message.data.metadata.table_schema
    end

    test "functions a consumer message with changes path" do
      message = ConsumersFactory.consumer_message(message_kind: :event)
      path_function = FunctionsFactory.insert_path_function!(function_attrs: [path: "changes"])
      consumer = %SinkConsumer{transform: path_function}

      result = Transforms.Message.to_external(consumer, message)
      assert result == message.data.changes
    end

    test "functions a consumer record .changes to null" do
      message = ConsumersFactory.consumer_message(message_kind: :record)
      path_function = FunctionsFactory.insert_path_function!(function_attrs: [path: "changes"])
      consumer = %SinkConsumer{transform: path_function}

      result = Transforms.Message.to_external(consumer, message)
      assert result == nil
    end

    test "functions a consumer message with action path" do
      message = ConsumersFactory.consumer_message(message_kind: :event)
      path_function = FunctionsFactory.insert_path_function!(function_attrs: [path: "action"])
      consumer = %SinkConsumer{transform: path_function}

      result = Transforms.Message.to_external(consumer, message)
      assert result == message.data.action
    end

    test "functions a consumer message with transaction annotations path" do
      message = ConsumersFactory.consumer_message(message_kind: :event)
      path_function = FunctionsFactory.insert_path_function!(function_attrs: [path: "metadata.transaction_annotations"])
      consumer = %SinkConsumer{transform: path_function}

      result = Transforms.Message.to_external(consumer, message)
      assert result == message.data.metadata.transaction_annotations
    end

    test "functions a consumer message with consumer path" do
      message = ConsumersFactory.consumer_message()
      path_function = FunctionsFactory.insert_path_function!(function_attrs: [path: "metadata.consumer"])
      consumer = %SinkConsumer{transform: path_function}

      result = Transforms.Message.to_external(consumer, message)
      assert result == message.data.metadata.consumer |> Map.from_struct() |> Sequin.Map.stringify_keys()
    end

    test "functions a consumer message with consumer name path" do
      message = ConsumersFactory.consumer_message()
      path_function = FunctionsFactory.insert_path_function!(function_attrs: [path: "metadata.consumer.name"])
      consumer = %SinkConsumer{transform: path_function}

      result = Transforms.Message.to_external(consumer, message)
      assert result == message.data.metadata.consumer.name
    end

    test "handles non-existent nested field gracefully" do
      message = ConsumersFactory.consumer_message(message_kind: :record)
      path_function = FunctionsFactory.insert_path_function!(function_attrs: [path: "record.nonexistent_field"])
      consumer = %SinkConsumer{transform: path_function}

      result = Transforms.Message.to_external(consumer, message)
      assert result == nil
    end

    test "handles non-existent deeply nested field gracefully" do
      message = ConsumersFactory.consumer_message(message_kind: :record)

      path_function =
        FunctionsFactory.insert_path_function!(function_attrs: [path: "record.nonexistent_field.nonexistent_field"])

      consumer = %SinkConsumer{transform: path_function}

      result = Transforms.Message.to_external(consumer, message)
      assert result == nil
    end

    test "handles non-existent metadata field gracefully" do
      message = ConsumersFactory.consumer_message(message_kind: :record)
      path_function = FunctionsFactory.insert_path_function!(function_attrs: [path: "metadata.nonexistent_field"])
      consumer = %SinkConsumer{transform: path_function}

      result = Transforms.Message.to_external(consumer, message)
      assert result == nil
    end

    test "handles non-existent transaction annotation field gracefully" do
      message = ConsumersFactory.consumer_message(message_kind: :record)

      path_function =
        FunctionsFactory.insert_path_function!(
          function_attrs: [path: "metadata.transaction_annotations.nonexistent_field"]
        )

      consumer = %SinkConsumer{transform: path_function}

      result = Transforms.Message.to_external(consumer, message)
      assert result == nil
    end

    test "handles non-existent sink field gracefully" do
      message = ConsumersFactory.consumer_message(message_kind: :record)
      path_function = FunctionsFactory.insert_path_function!(function_attrs: [path: "metadata.sink.nonexistent_field"])
      consumer = %SinkConsumer{transform: path_function}

      result = Transforms.Message.to_external(consumer, message)
      assert result == nil
    end
  end

  describe "transform function" do
    test "fails for unsaved function" do
      message = ConsumersFactory.consumer_message(message_kind: :event)

      transform =
        FunctionsFactory.insert_transform_function!(function_attrs: [body: ~s(%{it: record["column"]})])

      consumer = %SinkConsumer{transform: transform}
      result = Transforms.Message.to_external(consumer, message)

      colval = message.data.record["column"]
      assert %{it: ^colval} = result
    end

    test "compiler worker creates and updates" do
      account = AccountsFactory.insert_account!()

      transform_attrs =
        FunctionsFactory.function_attrs(function_type: :transform, function_attrs: [body: "1"])

      assert {:ok, xf} = Consumers.create_function(account.id, transform_attrs)

      assert_enqueued(worker: CLEW, args: %{"event" => "create"})

      Oban.drain_queue(queue: :lifecycle)

      assert {:ok, mod} = MiniElixir.module_name_from_id(xf.id)
      assert Code.loaded?(mod)
      md5 = mod.__info__(:md5)

      assert {:ok, _} =
               Consumers.update_function(
                 account.id,
                 xf.id,
                 FunctionsFactory.function_attrs(function_type: :transform, function_attrs: [body: "2"])
               )

      assert_enqueued(worker: CLEW, args: %{"event" => "update"})

      Oban.drain_queue(queue: :lifecycle)

      refute md5 == mod.__info__(:md5)
    end

    test "no junk in the database" do
      refute_enqueued(worker: CLEW)
    end

    test "compile transparently on first use when we wake up" do
      account = AccountsFactory.insert_account!()

      assert {:ok, xf} =
               Consumers.create_function(
                 account.id,
                 FunctionsFactory.function_attrs(function_type: :transform, function_attrs: [body: "1"])
               )

      consumer = %SinkConsumer{transform: xf}
      message = ConsumersFactory.consumer_message(message_kind: :event)
      result = Transforms.Message.to_external(consumer, message)
      assert 1 == result
    end

    test "error reporting line number" do
      account = AccountsFactory.insert_account!()

      assert {:ok, xf} =
               Consumers.create_function(
                 account.id,
                 FunctionsFactory.function_attrs(
                   function_type: :transform,
                   function_attrs: [body: ~s{q = record["nothing"]\nMap.get(q, "q")}]
                 )
               )

      consumer = %SinkConsumer{transform: xf}
      message = ConsumersFactory.consumer_message(message_kind: :event)

      ex =
        assert_raise Sequin.Error.ServiceError, fn ->
          Transforms.Message.to_external(consumer, message)
        end

      assert Exception.message(ex) =~ "line: 3"
    end

    test "access sink annotations" do
      account = AccountsFactory.insert_account!()

      assert {:ok, xf} =
               Consumers.create_function(
                 account.id,
                 FunctionsFactory.function_attrs(
                   function_type: :transform,
                   function_attrs: [body: "metadata.consumer.annotations"]
                 )
               )

      consumer = %SinkConsumer{transform: xf}
      message = ConsumersFactory.consumer_message(message_kind: :event)
      result = Transforms.Message.to_external(consumer, message)
      assert %{"test" => true} == result
    end
  end
end
