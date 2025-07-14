defmodule Sequin.YamlLoaderTest do
  use Sequin.DataCase, async: false

  alias Sequin.Accounts.Account
  alias Sequin.Accounts.User
  alias Sequin.ApiTokens
  alias Sequin.Consumers
  alias Sequin.Consumers.ElasticsearchSink
  alias Sequin.Consumers.Function
  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.KinesisSink
  alias Sequin.Consumers.RedisStreamSink
  alias Sequin.Consumers.RedisStringSink
  alias Sequin.Consumers.S2Sink
  alias Sequin.Consumers.SequinStreamSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SnsSink
  alias Sequin.Consumers.SourceTable, as: ConsumersSourceTable
  alias Sequin.Consumers.SqsSink
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabasePrimary
  alias Sequin.Error.BadRequestError
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.FunctionsFactory
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Replication.WalPipeline
  alias Sequin.Test.UnboxedRepo
  alias Sequin.TestSupport.Models.Character
  alias Sequin.TestSupport.ReplicationSlots
  alias Sequin.WalPipeline.SourceTable, as: WalPipelineSourceTable
  alias Sequin.YamlLoader

  @moduletag :unboxed

  @publication "characters_publication"

  def replication_slot, do: ReplicationSlots.slot_name(__MODULE__)

  setup do
    Application.put_env(:sequin, :self_hosted, true)

    # Fast-forward the replication slot to the current WAL position
    :ok = ReplicationSlots.reset_slot(UnboxedRepo, replication_slot())

    :ok
  end

  def playground_yml do
    """
    account:
      name: "Playground"

    users:
      - email: "admin@sequinstream.com"
        password: "sequinpassword!"

    databases:
      - name: "test-db"
        username: "postgres"
        password: "postgres"
        hostname: "localhost"
        database: "sequin_test"
        slot_name: "#{replication_slot()}"
        publication_name: "#{@publication}"
    """
  end

  describe "plan_from_yml" do
    test "returns lists of planned and current resources" do
      assert {:ok, planned_resources, [] = _current_resources, [] = _actions} =
               YamlLoader.plan_from_yml("""
                account:
                  name: "Playground"

                users:
                 - email: "admin@sequinstream.com"
                   password: "sequinpassword!"

                databases:
                  - name: "test-db"
                    username: "postgres"
                    password: "postgres"
                    hostname: "localhost"
                    database: "sequin_test"
                    slot_name: "#{replication_slot()}"
                    publication_name: "#{@publication}"

                http_endpoints:
                  - name: "test-endpoint"
                    url: "https://api.example.com/webhook"

                change_retentions:
                  - name: "test-pipeline"
                    source_database: "test-db"
                    source_table_schema: "public"
                    source_table_name: "Characters"
                    destination_database: "test-db"
                    destination_table_schema: "public"
                    destination_table_name: "sequin_events"
                    actions:
                      - insert
                      - delete
                    filters:
                      - column_name: "house"
                        operator: "="
                        comparison_value: "Atreides"
               """)

      account = Enum.find(planned_resources, &is_struct(&1, Account))
      assert account.name == "Playground"

      user = Enum.find(planned_resources, &is_struct(&1, User))
      assert user.email == "admin@sequinstream.com"

      database = Enum.find(planned_resources, &is_struct(&1, PostgresDatabase))
      database = Repo.preload(database, [:replication_slot])
      assert database.name == "test-db"

      http_endpoint = Enum.find(planned_resources, &is_struct(&1, HttpEndpoint))
      assert http_endpoint.name == "test-endpoint"
      assert http_endpoint.scheme == :https
      assert http_endpoint.host == "api.example.com"
      assert http_endpoint.path == "/webhook"
      assert http_endpoint.port == 443

      wal_pipeline = Enum.find(planned_resources, &is_struct(&1, WalPipeline))

      assert wal_pipeline.name == "test-pipeline"
      assert wal_pipeline.status == :active
      assert [%WalPipelineSourceTable{} = source_table] = wal_pipeline.source_tables
      assert source_table.oid == Character.table_oid()
      assert source_table.actions == [:insert, :delete]
    end

    test "returns invalid changeset for invalid database" do
      assert {:error, %BadRequestError{} = error} =
               YamlLoader.plan_from_yml("""
               account:
                 name: "Test Account"

               databases:
                 - name: "invalid-db"
               """)

      assert Exception.message(error) =~ "database: can't be blank"
    end
  end

  describe "playground.yml" do
    test "creates database with no existing account" do
      assert :ok = YamlLoader.apply_from_yml!(playground_yml())

      assert [account] = Repo.all(Account)
      assert account.name == "Playground"

      assert [%PostgresDatabase{} = db] = Repo.all(PostgresDatabase)
      assert db.account_id == account.id
      assert db.name == "test-db"

      assert [%PostgresReplicationSlot{} = replication] = Repo.all(PostgresReplicationSlot)
      assert replication.postgres_database_id == db.id
      assert replication.slot_name == replication_slot()
      assert replication.publication_name == @publication
    end

    test "applying yml twice creates no duplicates" do
      assert :ok = YamlLoader.apply_from_yml!(playground_yml())
      assert :ok = YamlLoader.apply_from_yml!(playground_yml())

      assert [account] = Repo.all(Account)
      assert account.name == "Playground"

      assert [%PostgresDatabase{} = db] = Repo.all(PostgresDatabase)
      assert db.account_id == account.id
      assert db.name == "test-db"

      assert [%PostgresReplicationSlot{} = replication] = Repo.all(PostgresReplicationSlot)
      assert replication.postgres_database_id == db.id
      assert replication.slot_name == replication_slot()
      assert replication.publication_name == @publication
    end
  end

  describe "databases" do
    test "creates a database" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               databases:
                 - name: "test-db"
                   username: "postgres"
                   password: "postgres"
                   hostname: "localhost"
                   database: "sequin_test"
                   slot_name: "#{replication_slot()}"
                   publication_name: "#{@publication}"
               """)

      assert [account] = Repo.all(Account)
      assert account.name == "Configured by Sequin"

      assert [%PostgresDatabase{} = db] = Repo.all(PostgresDatabase)
      assert db.account_id == account.id
      assert db.name == "test-db"
    end

    test "creates a database (backwards compatible with specified tables)" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               databases:
                 - name: "test-db"
                   username: "postgres"
                   password: "postgres"
                   hostname: "localhost"
                   database: "sequin_test"
                   slot_name: "#{replication_slot()}"
                   publication_name: "#{@publication}"
                   tables:
                     - name: "Characters"
                       schema: "public"
               """)

      assert [account] = Repo.all(Account)
      assert account.name == "Configured by Sequin"

      assert [%PostgresDatabase{} = db] = Repo.all(PostgresDatabase)
      assert db.account_id == account.id
      assert db.name == "test-db"
    end

    test "updates a database" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               databases:
                 - name: "test-db"
                   username: "postgres"
                   password: "postgres"
                   hostname: "localhost"
                   database: "sequin_test"
                   slot_name: "#{replication_slot()}"
                   publication_name: "#{@publication}"
               """)

      assert [%PostgresDatabase{} = db] = Repo.all(PostgresDatabase)
      assert db.pool_size == 10

      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               databases:
                 - name: "test-db"
                   username: "postgres"
                   password: "postgres"
                   hostname: "localhost"
                   database: "sequin_test"
                   pool_size: 5
                   slot_name: "#{replication_slot()}"
                   publication_name: "#{@publication}"
               """)

      assert [%PostgresDatabase{} = db] = Repo.all(PostgresDatabase)
      assert db.name == "test-db"
      assert db.pool_size == 5
    end

    test "creates database with block format publication (create_if_not_exists=false)" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               databases:
                 - name: "test-db"
                   username: "postgres"
                   password: "postgres"
                   hostname: "localhost"
                   database: "sequin_test"
                   slot:
                     name: "#{replication_slot()}"
                     create_if_not_exists: false
                   publication:
                     name: "#{@publication}"
                     create_if_not_exists: false
               """)

      assert [account] = Repo.all(Account)
      assert account.name == "Configured by Sequin"

      assert [%PostgresDatabase{} = db] = Repo.all(PostgresDatabase)
      assert db.account_id == account.id
      assert db.name == "test-db"

      assert [%PostgresReplicationSlot{} = replication] = Repo.all(PostgresReplicationSlot)
      assert replication.postgres_database_id == db.id
      assert replication.slot_name == replication_slot()
      assert replication.publication_name == @publication
    end

    test "creates database with block format publication (create_if_not_exists=true)" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               databases:
                 - name: "test-db"
                   username: "postgres"
                   password: "postgres"
                   hostname: "localhost"
                   database: "sequin_test"
                   slot:
                     name: "#{replication_slot()}"
                     create_if_not_exists: false
                   publication:
                     name: "#{@publication}"
                     create_if_not_exists: true
                     init_sql: |-
                       create publication #{@publication} for tables in schema public with (publish_via_partition_root = true)
               """)

      assert [account] = Repo.all(Account)
      assert account.name == "Configured by Sequin"

      assert [%PostgresDatabase{} = db] = Repo.all(PostgresDatabase)
      assert db.account_id == account.id
      assert db.name == "test-db"

      assert [%PostgresReplicationSlot{} = replication] = Repo.all(PostgresReplicationSlot)
      assert replication.postgres_database_id == db.id
      assert replication.slot_name == replication_slot()
      assert replication.publication_name == @publication

      # Verify that the publication was actually created
      # This assumes there's a way to check for the publication's existence in your test environment
      {:ok, conn} = Sequin.Databases.ConnectionCache.connection(db)
      {:ok, pub_info} = Sequin.Postgres.get_publication(conn, @publication)
      assert pub_info["pubname"] == @publication
    end

    test "fails with error when both block and flat publication formats are used" do
      assert {:error, error} =
               YamlLoader.apply_from_yml("""
               account:
                 name: "Configured by Sequin"

               databases:
                 - name: "test-db"
                   username: "postgres"
                   password: "postgres"
                   hostname: "localhost"
                   database: "sequin_test"
                   slot_name: "#{replication_slot()}"
                   publication_name: "#{@publication}"
                   publication:
                     name: "#{@publication}"
                     create_if_not_exists: true
               """)

      assert error.summary =~
               "Invalid database configuration: `publication` and `publication_name` are both specified. Only `publication` should be used."
    end
  end

  describe "http_endpoints" do
    test "creates webhook.site endpoint" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               http_endpoints:
                 - name: "webhook-endpoint"
                   webhook.site: "true"
               """)

      assert [endpoint] = Repo.all(HttpEndpoint)
      assert endpoint.name == "webhook-endpoint"
      assert endpoint.scheme == :https
      assert endpoint.host == "webhook.site"
      assert "/" <> uuid = endpoint.path
      assert Sequin.String.uuid?(uuid)
    end

    test "creates local endpoint" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               http_endpoints:
                 - name: "local-endpoint"
                   local: "true"
               """)

      assert [endpoint] = Repo.all(HttpEndpoint)
      assert endpoint.name == "local-endpoint"
      assert endpoint.use_local_tunnel == true
      refute endpoint.path
      assert endpoint.headers == %{}
      assert endpoint.encrypted_headers == %{}
    end

    test "creates local endpoint with options" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               http_endpoints:
                 - name: "local-endpoint"
                   local: "true"
                   path: "/webhook"
                   headers:
                     - key: "X-Test"
                       value: "test-value"
                   encrypted_headers:
                     - key: "X-Secret"
                       value: "secret-value"
               """)

      assert [endpoint] = Repo.all(HttpEndpoint)
      assert endpoint.name == "local-endpoint"
      assert endpoint.use_local_tunnel == true
      assert endpoint.path == "/webhook"
      assert endpoint.headers == %{"X-Test" => "test-value"}
      assert endpoint.encrypted_headers == %{"X-Secret" => "secret-value"}
    end

    test "creates external endpoint" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               http_endpoints:
                 - name: "external-endpoint"
                   url: "https://api.example.com:8443/webhooks?key=value#fragment"
                   headers:
                     - key: "Authorization"
                       value: "Bearer token"
                   encrypted_headers:
                     - key: "X-Secret"
                       value: "secret-value"
               """)

      assert [endpoint] = Repo.all(HttpEndpoint)
      assert endpoint.name == "external-endpoint"
      assert endpoint.scheme == :https
      assert endpoint.host == "api.example.com"
      assert endpoint.port == 8443
      assert endpoint.path == "/webhooks"
      assert endpoint.query == "key=value"
      assert endpoint.fragment == "fragment"
      assert endpoint.headers == %{"Authorization" => "Bearer token"}
      assert endpoint.encrypted_headers == %{"X-Secret" => "secret-value"}
    end

    test "applying yml twice creates no duplicates" do
      yaml = """
      account:
        name: "Configured by Sequin"

      http_endpoints:
        - name: "test-endpoint"
          url: "https://api.example.com/webhook"
      """

      assert :ok = YamlLoader.apply_from_yml!(yaml)
      assert :ok = YamlLoader.apply_from_yml!(yaml)

      assert [endpoint] = Repo.all(HttpEndpoint)
      assert endpoint.name == "test-endpoint"
    end

    test "validates required fields" do
      assert_raise RuntimeError, ~r/Invalid HTTP endpoint configuration/, fn ->
        YamlLoader.apply_from_yml!("""
        account:
          name: "Configured by Sequin"

        http_endpoints:
          - name: "invalid-endpoint"
        """)
      end
    end
  end

  describe "functions" do
    test "creates a function" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               functions:
                 - name: "my-record-only"
                   function:
                      type: "path"
                      path: "record"
               """)

      assert [function] = Repo.all(Function)
      assert function.name == "my-record-only"
      assert function.type == "path"
      assert function.function.path == "record"
    end

    test "creates a function flat format" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               functions:
                 - name: "my-record-only"
                   type: "path"
                   path: "record"
               """)

      assert [function] = Repo.all(Function)
      assert function.name == "my-record-only"
      assert function.type == "path"
      assert function.function.path == "record"
    end

    test "docs test" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               functions:
                 # Path function example
                 - name: "my-path-function"
                   description: "Extract record"       # Optional
                   type: "path"
                   path: "record"                     # Required for path functions

                 # Transform function example
                 - name: "my-transform-function"
                   description: "Extract ID and action"
                   type: "transform"
                   code: |-                          # Required for transform functions
                     def transform(action, record, changes, metadata) do
                       %{
                         id: record["id"],
                         action: action
                       }
                     end

                 # Filter function example
                 - name: "my-filter-function"
                   description: "Filter VIP customers"
                   type: "filter"
                   code: |-                          # Required for filter functions
                     def filter(action, record, changes, metadata) do
                       record["customer_type"] == "VIP"
                     end

                 # Routing function example
                 - name: "my-routing-function"
                   description: "Route to REST API"
                   type: "routing"
                   sink_type: "webhook"              # Required, sink type to route to
                   code: |-                          # Required for routing functions
                     def route(action, record, changes, metadata) do
                       %{
                         method: "POST",
                         endpoint_path: "/api/users/\#{record["id"]}"
                       }
                     end
               """)
    end

    test "creates a function backwards compatible with transforms" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               transforms:
                 - name: "my-path-transform"           # Required, unique name for this transform
                   description: "Extract record"       # Optional, description of the transform
                   transform:                          # Required, transform configuration
                     type: "path"                      # Required, "path" or "function"
                     path: "record"                    # Optional, path to extract from the message (Required if type is "path")
                 - name: "my-function-transform"
                   transform:
                     type: "function"
                     code: |-                          # Optional, Elixir code to transform the message (Required if type is "function")
                       def transform(action, record, changes, metadata) do
                         %{id: record["id"], action: action}
                       end
               """)

      assert [function1, function2] = Repo.all(Function, order_by: :name)
      assert function1.name == "my-path-transform"
      assert function1.type == "path"
      assert function1.function.path == "record"

      assert function2.name == "my-function-transform"
      assert function2.type == "transform"

      assert function2.function.code ==
               "def transform(action, record, changes, metadata) do\n  %{id: record[\"id\"], action: action}\nend"
    end

    test "updates an existing function" do
      # First create the function
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               functions:
                 - name: "my-function"
                   function:
                    type: "path"
                    path: "record"
               """)

      # Then update it
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               functions:
                 - name: "my-function"
                   function:
                     type: "path"
                     path: "record.id"
               """)

      assert [function] = Repo.all(Function)
      assert function.name == "my-function"
      assert function.type == "path"
      assert function.function.path == "record.id"
    end

    test "creates multiple functions" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               functions:
                 - name: "function-1"
                   function:
                     type: "path"
                     path: "record"
                 - name: "function-2"
                   function:
                     type: "path"
                     path: "record.status"
               """)

      assert functions = Repo.all(Function)
      assert length(functions) == 2

      function1 = Enum.find(functions, &(&1.name == "function-1"))
      assert function1.type == "path"
      assert function1.function.path == "record"

      function2 = Enum.find(functions, &(&1.name == "function-2"))
      assert function2.type == "path"
      assert function2.function.path == "record.status"
    end

    test "handles invalid function type" do
      assert_raise RuntimeError, ~r/Error creating function/, fn ->
        YamlLoader.apply_from_yml!("""
        account:
          name: "Configured by Sequin"

        functions:
          - name: "invalid-function"
            function:
              type: "invalid_type"
        """)
      end
    end

    test "creates a transform function" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               functions:
                 - name: "id-action-transform"
                   description: "Record ID and action"
                   function:
                     type: "transform"
                     code: |-
                        def transform(action, record, changes, metadata) do
                          %{
                            id: record["id"],
                            action: action
                          }
                        end
               """)

      assert [function] = Repo.all(Function)
      assert function.name == "id-action-transform"
      assert function.type == "transform"
      assert function.description == "Record ID and action"

      assert function.function.code ==
               String.trim("""
               def transform(action, record, changes, metadata) do
                 %{
                   id: record["id"],
                   action: action
                 }
               end
               """)
    end

    test "removing transform/filter/routing keys will remove attached functions from a sink" do
      account = AccountsFactory.insert_account!()
      table_attrs = DatabasesFactory.table_attrs()
      database = DatabasesFactory.insert_postgres_database!(account_id: account.id, tables: [table_attrs])

      transform = FunctionsFactory.insert_transform_function!(account_id: account.id)
      filter = FunctionsFactory.insert_filter_function!(account_id: account.id)
      routing = FunctionsFactory.insert_routing_function!(account_id: account.id)

      sink =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :redis_string,
          postgres_database_id: database.id,
          transform_id: transform.id,
          filter_id: filter.id,
          routing_mode: :dynamic,
          routing_id: routing.id
        )

      sink = Repo.preload(sink, [:transform, :filter, :routing, :postgres_database])

      assert %Function{} = sink.transform
      assert %Function{} = sink.filter
      assert %Function{} = sink.routing

      database_name = sink.postgres_database.name

      assert :ok =
               YamlLoader.apply_from_yml!(account.id, """
               sinks:
                 - name: #{sink.name}
                   database: #{database_name}
                   destination:
                     type: "redis_string"
                     host: #{sink.sink.host}
                     port: #{sink.sink.port}
               """)

      sink = sink |> Repo.reload() |> Repo.preload([:transform, :filter, :routing])

      assert is_nil(sink.transform)
      assert is_nil(sink.filter)
      assert is_nil(sink.routing)
    end

    test "rejects functions with file parameter" do
      assert_raise RuntimeError, ~r/`file` parameter found/, fn ->
        YamlLoader.apply_from_yml!("""
        account:
          name: "Configured by Sequin"

        functions:
          - name: "my-function"
            file: "my-function.ex"
        """)
      end
    end
  end

  describe "sinks" do
    def account_and_db_yml do
      """
      account:
        name: "Configured by Sequin"

      databases:
        - name: "test-db"
          hostname: "localhost"
          database: "sequin_test"
          slot_name: "#{replication_slot()}"
          publication_name: "#{@publication}"
      """
    end

    test "creates webhook subscription" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               functions:
                 - name: "record_only"
                   function:
                     type: "path"
                     path: "record"

               http_endpoints:
                 - name: "sequin-playground-http"
                   url: "https://api.example.com/webhook"

               sinks:
                 - name: "sequin-playground-webhook"
                   database: "test-db"
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   transform: "record_only"
                   max_retry_count: 5
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, [:transform])

      assert consumer.name == "sequin-playground-webhook"
      assert consumer.transform.name == "record_only"
      assert consumer.sink.batch == true
      assert consumer.max_retry_count == 5
      assert is_nil(consumer.source)
    end

    test "creates webhook subscription with batch=false" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               http_endpoints:
                 - name: "sequin-playground-http"
                   url: "https://api.example.com/webhook"

               sinks:
                 - name: "sequin-playground-webhook"
                   database: "test-db"
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                     batch: false
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      assert consumer.sink.batch == false
      assert is_nil(consumer.max_retry_count)
    end

    test "applying yml twice creates no duplicates" do
      yaml = """
      #{account_and_db_yml()}

      http_endpoints:
        - name: "sequin-playground-http"
          url: "https://api.example.com/webhook"

      sinks:
        - name: "sequin-playground-webhook"
          database: "test-db"
          destination:
            type: "webhook"
            http_endpoint: "sequin-playground-http"
        - name: "sequin-playground-kafka"
          database: "test-db"
          destination:
            type: "kafka"
            hosts: "localhost:9092"
            topic: "test-topic"
      """

      assert :ok = YamlLoader.apply_from_yml!(yaml)
      assert :ok = YamlLoader.apply_from_yml!(yaml)

      assert consumers = Repo.all(SinkConsumer)

      assert http_push_consumer = Enum.find(consumers, &(&1.sink.type == :http_push))
      assert http_push_consumer.name == "sequin-playground-webhook"

      assert kafka_consumer = Enum.find(consumers, &(&1.sink.type == :kafka))
      assert kafka_consumer.name == "sequin-playground-kafka"
    end

    test "updates webhook subscription" do
      create_yaml = """
      #{account_and_db_yml()}

      http_endpoints:
        - name: "sequin-playground-http"
          url: "https://api.example.com/webhook"

      sinks:
        - name: "sequin-playground-webhook"
          database: "test-db"
          destination:
            type: "webhook"
            http_endpoint: "sequin-playground-http"
      """

      assert :ok = YamlLoader.apply_from_yml!(create_yaml)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = SinkConsumer.preload_http_endpoint!(consumer)

      assert consumer.name == "sequin-playground-webhook"
      assert consumer.sink.http_endpoint.name == "sequin-playground-http"

      update_yaml = """
      #{account_and_db_yml()}

      http_endpoints:
        - name: "new-http-endpoint"
          url: "https://api.example.com/webhook"

      sinks:
        - name: "sequin-playground-webhook"
          database: "test-db"
          destination:
            type: "webhook"
            http_endpoint: "new-http-endpoint"
      """

      # Update with different filters
      assert :ok = YamlLoader.apply_from_yml!(update_yaml)

      assert [updated_consumer] = Repo.all(SinkConsumer)
      updated_consumer = SinkConsumer.preload_http_endpoint!(updated_consumer)

      assert updated_consumer.name == "sequin-playground-webhook"
      assert updated_consumer.sink.http_endpoint.name == "new-http-endpoint"
    end

    test "creates sequin stream consumer" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sinks:
                 - name: "sequin-playground-consumer"
                   database: "test-db"
                   destination:
                     type: "sequin_stream"
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "sequin-playground-consumer"
      assert consumer.legacy_transform == :none

      assert %SequinStreamSink{} = consumer.sink
    end

    test "creates sink consumer with schema filter" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sinks:
                 - name: "schema-consumer"
                   database: "test-db"
                   source:
                     include_schemas: ["public"]
                   destination:
                     type: "sequin_stream"
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "schema-consumer"
      assert consumer.source.include_schemas == ["public"]
      assert %SequinStreamSink{} = consumer.sink
    end

    test "creates webhook subscription excluding schemas" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               http_endpoints:
                 - name: "sequin-playground-http"
                   url: "https://api.example.com/webhook"

               sinks:
                 - name: "schema-webhook-consumer"
                   database: "test-db"
                   source:
                     exclude_schemas: ["private"]
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "schema-webhook-consumer"
      assert consumer.source.exclude_schemas == ["private"]
      assert consumer.sink.type == :http_push
    end

    test "updates sink consumer from table to schema filter" do
      # First create consumer with table
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sinks:
                 - name: "update-consumer"
                   database: "test-db"
                   source:
                     include_tables: ["Characters"]
                   destination:
                     type: "sequin_stream"
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      assert [table_oid] = consumer.source.include_table_oids
      assert is_integer(table_oid)

      # Now update to use schema filter
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sinks:
                 - name: "update-consumer"
                   database: "test-db"
                   source:
                     include_schemas: ["public"]
                   destination:
                     type: "sequin_stream"
               """)

      assert [updated_consumer] = Repo.all(SinkConsumer)
      assert updated_consumer.source.include_schemas == ["public"]
      assert is_nil(updated_consumer.source.include_table_oids)
    end

    test "updates sink consumer from schema to table filter" do
      # First create consumer with schema
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sinks:
                 - name: "reverse-update-consumer"
                   database: "test-db"
                   source:
                     include_schemas: ["public"]
                   destination:
                     type: "sequin_stream"
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      assert consumer.source.include_schemas == ["public"]
      assert is_nil(consumer.source.include_table_oids)

      # Now update to use table filter
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sinks:
                 - name: "reverse-update-consumer"
                   database: "test-db"
                   source:
                     include_tables: ["Characters"]
                   destination:
                     type: "sequin_stream"
               """)

      assert [updated_consumer] = Repo.all(SinkConsumer)
      assert [table_oid] = updated_consumer.source.include_table_oids
      assert is_integer(table_oid)
      assert is_nil(updated_consumer.source.include_schemas)
    end

    test "fails when both include and exclude are specified" do
      assert_raise RuntimeError, ~r/cannot be set when include_schemas is set/, fn ->
        YamlLoader.apply_from_yml!("""
        #{account_and_db_yml()}

        sinks:
          - name: "invalid-consumer"
            database: "test-db"
            source:
              include_schemas: ["public"]
              exclude_schemas: ["private"]
            destination:
              type: "sequin_stream"
        """)
      end
    end

    test "sets group_column_attnums on a table" do
      YamlLoader.apply_from_yml!("""
      #{account_and_db_yml()}

      sinks:
        - name: "group-column-attnums-consumer"
          database: "test-db"
          tables:
            - name: "Characters"
              group_column_names: ["id"]
          destination:
            type: "sequin_stream"
      """)

      assert [consumer] = Repo.all(SinkConsumer)
      assert [%ConsumersSourceTable{group_column_attnums: [1]}] = consumer.source_tables
    end

    test "sets message_grouping field" do
      YamlLoader.apply_from_yml!("""
      #{account_and_db_yml()}

      sinks:
        - name: "message-grouping-consumer"
          database: "test-db"
          message_grouping: false
          tables:
            - name: "Characters"
          destination:
            type: "sequin_stream"
      """)

      assert [consumer] = Repo.all(SinkConsumer)
      assert consumer.message_grouping == false
    end

    test "fails when message_grouping is false but group_column_names are specified" do
      assert_raise RuntimeError, ~r/Cannot specify group columns when message_grouping is disabled/, fn ->
        YamlLoader.apply_from_yml!("""
        #{account_and_db_yml()}

        sinks:
          - name: "invalid-grouping-consumer"
            database: "test-db"
            message_grouping: false
            tables:
              - name: "Characters"
                group_column_names: ["id"]
            destination:
              type: "sequin_stream"
        """)
      end
    end

    test "fails when group_column_names contains non-existent column" do
      assert_raise RuntimeError, ~r/Failed to parse tables.*No `postgres column` found.*non_existent_column/, fn ->
        YamlLoader.apply_from_yml!("""
        #{account_and_db_yml()}

        sinks:
          - name: "invalid-consumer"
            database: "test-db"
            tables:
              - name: "Characters"
                group_column_names: ["id", "non_existent_column"]
            destination:
              type: "sequin_stream"
        """)
      end
    end

    test "creates kafka sink consumer" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sinks:
                 - name: "kafka-consumer"
                   database: "test-db"
                   destination:
                     type: "kafka"
                     hosts: "localhost:9092"
                     topic: "test-topic"
                     tls: false
                     username: "test-user"
                     password: "test-pass"
                     sasl_mechanism: "plain"
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "kafka-consumer"

      assert %KafkaSink{
               type: :kafka,
               hosts: "localhost:9092",
               topic: "test-topic",
               tls: false,
               username: "test-user",
               password: "test-pass",
               sasl_mechanism: :plain
             } = consumer.sink
    end

    test "creates sqs sink consumer" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sinks:
                 - name: "sqs-consumer"
                   database: "test-db"
                   destination:
                     type: "sqs"
                     queue_url: "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue.fifo"
                     access_key_id: "AKIAXXXXXXXXXXXXXXXX"
                     secret_access_key: "secret123"
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "sqs-consumer"

      assert %SqsSink{
               type: :sqs,
               queue_url: "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue.fifo",
               region: "us-west-2",
               access_key_id: "AKIAXXXXXXXXXXXXXXXX",
               secret_access_key: "secret123",
               is_fifo: true
             } = consumer.sink
    end

    test "creates sns sink consumer" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sinks:
                 - name: "sns-consumer"
                   database: "test-db"
                   destination:
                     type: "sns"
                     topic_arn: "arn:aws:sns:us-west-2:123456789012:MyTopic"
                     access_key_id: "AKIAXXXXXXXXXXXXXXXX"
                     secret_access_key: "secret123"
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "sns-consumer"

      assert %SnsSink{
               type: :sns,
               topic_arn: "arn:aws:sns:us-west-2:123456789012:MyTopic",
               region: "us-west-2",
               access_key_id: "AKIAXXXXXXXXXXXXXXXX",
               secret_access_key: "secret123"
             } = consumer.sink
    end

    test "creates kinesis sink consumer" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sinks:
                 - name: "kinesis-consumer"
                   database: "test-db"
                   destination:
                     type: "kinesis"
                     stream_arn: "arn:aws:kinesis:us-west-2:123456789012:stream/test"
                     access_key_id: "AKIAXXXXXXXXXXXXXXXX"
                     secret_access_key: "secret123"
                     routing_mode: "static"
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "kinesis-consumer"

      assert %KinesisSink{
               type: :kinesis,
               stream_arn: "arn:aws:kinesis:us-west-2:123456789012:stream/test",
               region: "us-west-2",
               access_key_id: "AKIAXXXXXXXXXXXXXXXX",
               secret_access_key: "secret123",
               routing_mode: :static
             } = consumer.sink
    end

    test "creates s2 sink consumer" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sinks:
                 - name: "s2-consumer"
                   database: "test-db"
                   destination:
                     type: "s2"
                     basin: "test-basin"
                     stream: "my-stream"
                     access_token: "tok"
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "s2-consumer"

      assert %S2Sink{type: :s2, basin: "test-basin", stream: "my-stream", access_token: "tok"} = consumer.sink
    end

    test "creates redis sink consumer" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sinks:
                 - name: "redis-consumer"
                   database: "test-db"
                   destination:
                     type: "redis_stream"
                     host: "localhost"
                     port: 6379
                     stream_key: "test-stream"
                     database: 1
                     tls: false
                     username: "test-user"
                     password: "test-pass"
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "redis-consumer"

      assert %RedisStreamSink{
               type: :redis_stream,
               host: "localhost",
               port: 6379,
               stream_key: "test-stream",
               database: 1,
               tls: false,
               username: "test-user",
               password: "test-pass"
             } = consumer.sink
    end

    test "creates gcp pubsub sink consumer" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sinks:
                 - name: "pubsub-consumer"
                   database: "test-db"
                   destination:
                     type: "gcp_pubsub"
                     project_id: "my-project"
                     topic_id: "my-topic"
                     credentials:
                       type: "service_account"
                       project_id: "my-project"
                       private_key_id: "key123"
                       private_key: "-----BEGIN PRIVATE KEY-----\\nMIIE...\\n-----END PRIVATE KEY-----\\n"
                       client_email: "my-service-account@my-project.iam.gserviceaccount.com"
                       client_id: "123456789"
                       auth_uri: "https://accounts.google.com/o/oauth2/auth"
                       token_uri: "https://oauth2.googleapis.com/token"
                       auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs"
                       client_x509_cert_url: "https://www.googleapis.com/robot/v1/metadata/x509/my-service-account%40my-project.iam.gserviceaccount.com"
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "pubsub-consumer"

      assert %GcpPubsubSink{
               type: :gcp_pubsub,
               project_id: "my-project",
               topic_id: "my-topic",
               credentials: %{
                 type: "service_account",
                 project_id: "my-project",
                 private_key_id: "key123",
                 private_key: "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----\n",
                 client_email: "my-service-account@my-project.iam.gserviceaccount.com",
                 client_id: "123456789",
                 auth_uri: "https://accounts.google.com/o/oauth2/auth",
                 token_uri: "https://oauth2.googleapis.com/token",
                 auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs",
                 client_x509_cert_url:
                   "https://www.googleapis.com/robot/v1/metadata/x509/my-service-account%40my-project.iam.gserviceaccount.com"
               }
             } = consumer.sink
    end

    test "creates elasticsearch sink consumer" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sinks:
                 - name: "elasticsearch-consumer"
                   database: "test-db"
                   destination:
                     type: "elasticsearch"
                     endpoint_url: "https://elasticsearch.example.com"
                     index_name: "test-index"
                     auth_type: "api_key"
                     auth_value: "sensitive-api-key"
                     batch_size: 100
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "elasticsearch-consumer"

      assert %ElasticsearchSink{
               type: :elasticsearch,
               endpoint_url: "https://elasticsearch.example.com",
               index_name: "test-index",
               auth_type: :api_key,
               auth_value: "sensitive-api-key",
               batch_size: 100
             } = consumer.sink
    end

    test "creates redis string sink consumer" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sinks:
                 - name: "redis-string-consumer"
                   database: "test-db"
                   destination:
                     type: "redis_string"
                     host: "redis-string.example.com"
                     port: 6379
                     username: "test-user"
                     password: "test-pass"
                     tls: false
                     database: 0
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "redis-string-consumer"

      assert %RedisStringSink{
               type: :redis_string,
               host: "redis-string.example.com",
               port: 6379,
               username: "test-user",
               password: "test-pass",
               tls: false,
               database: 0
             } = consumer.sink
    end

    test "handles removing and re-adding a sink after the table is renamed" do
      # First apply config with sink
      initial_yaml = """
      #{account_and_db_yml()}

      http_endpoints:
        - name: "sequin-playground-http"
          url: "https://api.example.com/webhook"

      sinks:
        - name: "sequin-playground-webhook"
          database: "test-db"
          source:
            include_tables: ["Characters"]
          tables:
            - name: "Characters"
              group_column_names: ["id"]
          destination:
            type: "webhook"
            http_endpoint: "sequin-playground-http"
      """

      assert :ok = YamlLoader.apply_from_yml!(initial_yaml)

      # Verify initial state
      assert [consumer] = Repo.all(SinkConsumer)
      assert consumer.source.include_table_oids == [Character.table_oid()]

      assert consumer.source_tables == [
               %ConsumersSourceTable{table_oid: Character.table_oid(), group_column_attnums: [1]}
             ]

      assert [db] = Repo.all(PostgresDatabase)

      # Remove the sink
      Consumers.delete_sink_consumer(consumer)

      # Verify sink was removed
      assert [] = Repo.all(SinkConsumer)

      # Re-apply original config with sink and a renamed table
      new_yaml = String.replace(initial_yaml, "Characters", "Characters_new")

      tables =
        Enum.map(db.tables, fn table ->
          table = Sequin.Map.from_struct_deep(table)

          if table.name == "Characters" do
            %{table | name: "Characters_new"}
          else
            table
          end
        end)

      Databases.update_db(db, %{tables: tables})

      assert :ok = YamlLoader.apply_from_yml!(new_yaml)

      # Verify final state
      assert [updated_consumer] = Repo.all(SinkConsumer)
      assert updated_consumer.source.include_table_oids == [Character.table_oid()]

      assert updated_consumer.source_tables == [
               %ConsumersSourceTable{table_oid: Character.table_oid(), group_column_attnums: [1]}
             ]
    end

    test "creates multiple sinks using YAML anchors" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sink_template: &sink_template
                 status: active
                 transform: "none"
                 destination:
                   type: gcp_pubsub
                   emulator_base_url: http://localhost:8085
                   project_id: my-project-id
                   topic_id: my-topic
                   use_emulator: true
                 database: test-db

               sinks:
                 - <<: *sink_template
                   name: gcp-events-characters
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "gcp-events-characters"
      assert consumer.status == :active
      assert consumer.legacy_transform == :none

      assert %GcpPubsubSink{
               type: :gcp_pubsub,
               project_id: "my-project-id",
               topic_id: "my-topic",
               use_emulator: true,
               emulator_base_url: "http://localhost:8085"
             } = consumer.sink
    end

    test "creates sink with custom actions and timestamp format" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               http_endpoints:
                - name: "sequin-playground-http"
                  url: "https://api.example.com/webhook"

               sinks:
                 - name: "custom-actions-sink"
                   database: "test-db"
                   actions:
                     - insert
                     - delete
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   timestamp_format: "unix_microsecond"
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "custom-actions-sink"
      assert consumer.actions == [:insert, :delete]
    end

    test "creates sink with source includes and excludes" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               http_endpoints:
                - name: "sequin-playground-http"
                  url: "https://api.example.com/webhook"

               sinks:
                 - name: "custom-actions-sink"
                   database: "test-db"
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   source:
                     include_tables:
                       - Characters
                       - characters_detailed
                     exclude_schemas: ["private"]
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "custom-actions-sink"
      assert length(consumer.source.include_table_oids) == 2
      assert Enum.all?(consumer.source.include_table_oids, &is_integer/1)
      assert consumer.source.exclude_schemas == ["private"]
    end

    test "creates multiple sinks with different names using YAML anchors" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               sink_template: &sink_template
                 status: active
                 transform: "none"
                 destination:
                   type: gcp_pubsub
                   emulator_base_url: http://localhost:8085
                   project_id: my-project-id
                   topic_id: my-topic
                   use_emulator: true
                 database: test-db

               sinks:
                 - <<: *sink_template
                   name: gcp-events-characters-1
                 - <<: *sink_template
                   name: gcp-events-characters-2
               """)

      assert consumers = Repo.all(SinkConsumer)
      assert length(consumers) == 2

      consumer_names = Enum.map(consumers, & &1.name)
      assert "gcp-events-characters-1" in consumer_names
      assert "gcp-events-characters-2" in consumer_names

      for consumer <- consumers do
        assert consumer.status == :active
        assert consumer.legacy_transform == :none

        assert %GcpPubsubSink{
                 type: :gcp_pubsub,
                 project_id: "my-project-id",
                 topic_id: "my-topic",
                 use_emulator: true,
                 emulator_base_url: "http://localhost:8085"
               } = consumer.sink
      end
    end

    test "creates webhook subscription with transform reference" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               functions:
                 - name: "my-transform"
                   function:
                    type: "path"
                    path: "record"

               http_endpoints:
                 - name: "sequin-playground-http"
                   url: "https://api.example.com/webhook"

               sinks:
                 - name: "sequin-playground-webhook"
                   database: "test-db"
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   transform: "my-transform"
               """)

      assert [function] = Repo.all(Function)
      assert function.name == "my-transform"
      assert function.type == "path"
      assert function.function.path == "record"

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "sequin-playground-webhook"
      assert consumer.transform_id == function.id
    end

    test "creates webhook subscription with transform function" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               functions:
                 - name: "id-action-transform"
                   description: "Extract ID and action"
                   function:
                     type: "transform"
                     code: |-
                       def transform(action, record, changes, metadata) do
                         %{
                           id: record["id"],
                           action: action
                         }
                       end

               http_endpoints:
                 - name: "sequin-playground-http"
                   url: "https://api.example.com/webhook"

               sinks:
                 - name: "sequin-playground-webhook"
                   database: "test-db"
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   transform: "id-action-transform"
               """)

      assert [function] = Repo.all(Function)
      assert function.name == "id-action-transform"
      assert function.type == "transform"
      assert function.description == "Extract ID and action"

      assert function.function.code ==
               String.trim("""
               def transform(action, record, changes, metadata) do
                 %{
                   id: record["id"],
                   action: action
                 }
               end
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "sequin-playground-webhook"
      assert consumer.transform_id == function.id
    end

    test "creates sink with no transform" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               http_endpoints:
                 - name: "sequin-playground-http"
                   url: "https://api.example.com/webhook"

               sinks:
                 - name: "sequin-playground-webhook"
                   database: "test-db"
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   transform: "none"
               """)

      assert [consumer] = Repo.all(SinkConsumer)

      assert consumer.name == "sequin-playground-webhook"
      assert consumer.transform_id == nil
    end

    test "raises error when referenced transform not found" do
      assert_raise RuntimeError, ~r/Function 'missing-function' not found/, fn ->
        YamlLoader.apply_from_yml!("""
        #{account_and_db_yml()}

        http_endpoints:
          - name: "sequin-playground-http"
            url: "https://api.example.com/webhook"

        sinks:
          - name: "sequin-playground-webhook"
            database: "test-db"
            destination:
              type: "webhook"
              http_endpoint: "sequin-playground-http"
            transform: "missing-function"
        """)
      end
    end

    test "creates webhook subscription with routing and filter functions" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               functions:
                 - name: "my-routing"
                   function:
                     type: "routing"
                     sink_type: "http_push"
                     code: |-
                       def route(action, record, changes, metadata) do
                         %{
                           method: "POST",
                           endpoint_path: "/custom/\#{record["id"]}"
                         }
                       end
                 - name: "my-filter"
                   function:
                     type: "filter"
                     code: |-
                       def filter(action, record, changes, metadata) do
                         true
                       end

               http_endpoints:
                 - name: "sequin-playground-http"
                   url: "https://api.example.com/webhook"

               sinks:
                 - name: "sequin-playground-webhook"
                   database: "test-db"
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   routing: "my-routing"
                   filter: "my-filter"
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, [:routing, :filter])

      assert consumer.name == "sequin-playground-webhook"

      # Check routing function reference was used
      assert consumer.routing
      assert consumer.routing.name == "my-routing"
      assert consumer.routing.function.type == :routing
      assert consumer.routing.function.sink_type == :http_push
      assert consumer.routing.function.code =~ "def route(action, record, changes, metadata)"
      assert consumer.routing.function.code =~ "/custom/"

      # Check filter function reference was used
      assert consumer.filter
      assert consumer.filter.name == "my-filter"
      assert consumer.filter.function.type == :filter
      assert consumer.filter.function.code =~ "def filter(action, record, changes, metadata)"
      assert consumer.filter.function.code =~ "true"
    end

    test "creates webhook subscription with new yaml names" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               functions:
                 - name: "my-routing"
                   function:
                     type: "routing"
                     sink_type: "http_push"
                     code: |-
                       def route(action, record, changes, metadata) do
                         %{
                           method: "POST",
                           endpoint_path: "/custom/\#{record["id"]}"
                         }
                       end

               http_endpoints:
                 - name: "sequin-playground-http"
                   url: "https://api.example.com/webhook"

               sinks:
                 - name: "sequin-playground-webhook"
                   database: "test-db"
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   routing: "my-routing"
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, [:routing])

      assert consumer.routing.name == "my-routing"
    end

    test "creates webhook subscription with FLAT YAML" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_and_db_yml()}

               functions:
                 - name: "my-routing"
                   type: "routing"
                   sink_type: "http_push"
                   code: |-
                     def route(action, record, changes, metadata) do
                       %{
                         method: "POST",
                         endpoint_path: "/custom/\#{record["id"]}"
                       }
                     end

               http_endpoints:
                 - name: "sequin-playground-http"
                   url: "https://api.example.com/webhook"

               sinks:
                 - name: "sequin-playground-webhook"
                   database: "test-db"
                   source:
                     include_tables: ["Characters"]
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   routing: "my-routing"
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, [:routing])

      assert consumer.routing.name == "my-routing"
    end

    test "errors when routing function doesn't exist" do
      assert_raise RuntimeError, ~r/[Ff]unction 'non-existent-routing' not found/, fn ->
        YamlLoader.apply_from_yml!("""
        #{account_and_db_yml()}

        http_endpoints:
          - name: "sequin-playground-http"
            url: "https://api.example.com/webhook"

        sinks:
          - name: "sequin-playground-webhook"
            database: "test-db"
            destination:
              type: "webhook"
              http_endpoint: "sequin-playground-http"
            routing: "non-existent-routing"
        """)
      end
    end

    test "errors when function referenced for routing is not a routing function" do
      assert_raise RuntimeError, "`routing` must reference a function with type `routing`", fn ->
        YamlLoader.apply_from_yml!("""
        #{account_and_db_yml()}

        functions:
          - name: "regular-transform"
            function:
              type: "path"
              path: "record"

        http_endpoints:
          - name: "sequin-playground-http"
            url: "https://api.example.com/webhook"

        sinks:
          - name: "sequin-playground-webhook"
            database: "test-db"
            destination:
              type: "webhook"
              http_endpoint: "sequin-playground-http"
            routing: "regular-transform"
        """)
      end
    end
  end

  describe "await_database" do
    test "retries with default values when db not immediately available" do
      # Connection attempt counter using atomics
      counter = :atomics.new(1, [])

      # Mock test_connect that fails first time, succeeds second time
      test_connect_fun = fn _db ->
        count = :atomics.add_get(counter, 1, 1)

        if count == 1 do
          {:error, "Connection refused"}
        else
          :ok
        end
      end

      result =
        YamlLoader.apply_from_yml(
          nil,
          """
          account:
            name: "Test Account"

          databases:
            - name: "test-db"
              username: "postgres"
              password: "postgres"
              hostname: "localhost"
              database: "sequin_test"
              slot_name: "#{replication_slot()}"
              publication_name: "#{@publication}"
          """,
          test_connect_fun: test_connect_fun
        )

      assert {:ok, _resources} = result

      # Should have attempted exactly twice
      assert :atomics.get(counter, 1) == 2
    end

    @tag capture_log: true
    test "returns error when timeout is reached" do
      # Track attempts using atomics
      counter = :atomics.new(1, [])
      test_pid = self()

      # Mock test_connect that always fails
      test_connect_fun = fn _db ->
        :atomics.add_get(counter, 1, 1)
        send(test_pid, :test_connect_called)
        {:error, "Connection refused"}
      end

      result =
        YamlLoader.apply_from_yml(
          nil,
          """
          account:
            name: "Test Account"

          databases:
            - name: "test-db"
              username: "postgres"
              password: "postgres"
              hostname: "localhost"
              database: "sequin_test"
              slot_name: "#{replication_slot()}"
              publication_name: "#{@publication}"
              await_database:
                timeout_ms: 5
                interval_ms: 2
          """,
          test_connect_fun: test_connect_fun
        )

      assert {:error, error} = result
      assert error.message =~ "Failed to connect to database 'test-db' after 5ms"

      # Verify we attempted at least once
      assert_received :test_connect_called
    end
  end

  describe "provisions api token" do
    test "creates local endpoint with options" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"
               api_tokens:
                 - name: "mytoken"
                   token: "secret"
               """)

      assert {:ok, token} = ApiTokens.find_by_token("secret")
      acct = Repo.one(Account, id: token.account_id)
      assert %Account{name: "Configured by Sequin"} = acct
      assert token.name == "mytoken"
    end

    test "cannot modify tokens" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"
               api_tokens:
                 - name: "mytoken"
                   token: "secret"
               """)

      assert {:error, %BadRequestError{}} =
               YamlLoader.apply_from_yml("""
               account:
                 name: "Configured by Sequin"
               api_tokens:
                 - name: "mytoken"
                   token: "other"
               """)
    end

    test "idempotency" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"
               api_tokens:
                 - name: "mytoken"
                   token: "secret"
               """)

      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"
               api_tokens:
                 - name: "mytoken"
                   token: "secret"
               """)
    end
  end

  describe "replica with primary" do
    test "creates a database with primary connection parameters" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               databases:
                 - name: "replica-db"
                   username: "postgres"
                   password: "postgres"
                   hostname: "localhost"
                   database: "sequin_test"
                   slot_name: "#{replication_slot()}"
                   publication_name: "#{@publication}"
                   primary:
                     username: "primary_user"
                     password: "primary_password"
                     hostname: "primary.example.com"
                     port: 5432
                     database: "primary_db"
               """)

      assert [%PostgresDatabase{} = db] = Repo.all(PostgresDatabase)

      assert db.name == "replica-db"
      assert db.hostname == "localhost"
      assert db.database == "sequin_test"

      # Verify primary connection
      assert %PostgresDatabasePrimary{} = db.primary
      assert db.primary.username == "primary_user"
      assert db.primary.password == "primary_password"
      assert db.primary.hostname == "primary.example.com"
      assert db.primary.port == 5432
      assert db.primary.database == "primary_db"
    end

    test "updates a database to add primary connection" do
      # First create a database without primary
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               databases:
                 - name: "replica-db"
                   username: "postgres"
                   password: "postgres"
                   hostname: "localhost"
                   database: "sequin_test"
                   slot_name: "#{replication_slot()}"
                   publication_name: "#{@publication}"
               """)

      # Verify initial state
      assert [%PostgresDatabase{} = db] = Repo.all(PostgresDatabase)
      assert is_nil(db.primary)

      # Now update to add primary
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               databases:
                 - name: "replica-db"
                   username: "postgres"
                   password: "postgres"
                   hostname: "localhost"
                   database: "sequin_test"
                   slot_name: "#{replication_slot()}"
                   publication_name: "#{@publication}"
                   primary:
                     username: "primary_user"
                     password: "primary_password"
                     hostname: "primary.example.com"
                     port: 5432
                     database: "primary_db"
               """)

      # Verify primary was added
      assert [%PostgresDatabase{} = updated_db] = Repo.all(PostgresDatabase)

      assert updated_db.name == "replica-db"
      assert %PostgresDatabasePrimary{} = updated_db.primary
      assert updated_db.primary.username == "primary_user"
      assert updated_db.primary.hostname == "primary.example.com"
    end

    test "updates existing primary connection parameters" do
      # First create a database with primary
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               databases:
                 - name: "replica-db"
                   username: "postgres"
                   password: "postgres"
                   hostname: "localhost"
                   database: "sequin_test"
                   slot_name: "#{replication_slot()}"
                   publication_name: "#{@publication}"
                   primary:
                     username: "primary_user"
                     password: "primary_password"
                     hostname: "primary.example.com"
                     port: 5432
                     database: "primary_db"
               """)

      # Verify initial state
      assert [%PostgresDatabase{} = db] = Repo.all(PostgresDatabase)
      assert db.primary.hostname == "primary.example.com"
      assert db.primary.port == 5432

      # Now update primary parameters
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               databases:
                 - name: "replica-db"
                   username: "postgres"
                   password: "postgres"
                   hostname: "localhost"
                   database: "sequin_test"
                   slot_name: "#{replication_slot()}"
                   publication_name: "#{@publication}"
                   primary:
                     username: "updated_user"
                     password: "updated_password"
                     hostname: "new-primary.example.com"
                     port: 6432
                     database: "primary_db"
               """)

      # Verify primary was updated
      assert [%PostgresDatabase{} = updated_db] = Repo.all(PostgresDatabase)

      assert updated_db.name == "replica-db"
      assert %PostgresDatabasePrimary{} = updated_db.primary
      assert updated_db.primary.username == "updated_user"
      assert updated_db.primary.password == "updated_password"
      assert updated_db.primary.hostname == "new-primary.example.com"
      assert updated_db.primary.port == 6432
    end

    test "creates a database with connection url and primary" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               databases:
                 - name: "url-replica-db"
                   url: "postgresql://postgres:postgres@localhost:5432/sequin_test"
                   slot_name: "#{replication_slot()}"
                   publication_name: "#{@publication}"
                   primary:
                     url: "postgresql://primary_user:primary_password@primary.example.com:5432/primary_db"
               """)

      assert [%PostgresDatabase{} = db] = Repo.all(PostgresDatabase)

      assert db.name == "url-replica-db"
      assert db.hostname == "localhost"
      assert db.database == "sequin_test"
      assert db.username == "postgres"
      assert db.password == "postgres"

      # Verify primary connection
      assert %PostgresDatabasePrimary{} = db.primary
      assert db.primary.username == "primary_user"
      assert db.primary.password == "primary_password"
      assert db.primary.hostname == "primary.example.com"
      assert db.primary.port == 5432
      assert db.primary.database == "primary_db"
    end
  end
end
