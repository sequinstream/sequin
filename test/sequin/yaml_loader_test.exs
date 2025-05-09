defmodule Sequin.YamlLoaderTest do
  use Sequin.DataCase, async: false

  alias Sequin.Accounts.Account
  alias Sequin.Accounts.User
  alias Sequin.ApiTokens
  alias Sequin.Consumers
  alias Sequin.Consumers.ElasticsearchSink
  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.RedisStreamSink
  alias Sequin.Consumers.RedisStringSink
  alias Sequin.Consumers.SequenceFilter
  alias Sequin.Consumers.SequenceFilter.NullValue
  alias Sequin.Consumers.SequenceFilter.StringValue
  alias Sequin.Consumers.SequinStreamSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.SnsSink
  alias Sequin.Consumers.SourceTable
  alias Sequin.Consumers.SqsSink
  alias Sequin.Consumers.Transform
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.PostgresDatabasePrimary
  alias Sequin.Databases.Sequence
  alias Sequin.Error.BadRequestError
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Replication.WalPipeline
  alias Sequin.Test.UnboxedRepo
  alias Sequin.TestSupport.Models.Character
  alias Sequin.TestSupport.ReplicationSlots
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
      assert {:ok, planned_resources, [] = _current_resources} =
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
      assert [%SourceTable{} = source_table] = wal_pipeline.source_tables
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
    test "creates database and sequence with no existing account" do
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

      db = Repo.preload(db, [:sequences])
      assert [] = db.sequences
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
      assert Sequin.String.is_uuid?(uuid)
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

  describe "transforms" do
    test "creates a transform" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               transforms:
                 - name: "my-record-only"
                   transform:
                      type: "path"
                      path: "record"
               """)

      assert [transform] = Repo.all(Transform)
      assert transform.name == "my-record-only"
      assert transform.type == "path"
      assert transform.transform.path == "record"
    end

    test "updates an existing transform" do
      # First create the transform
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               transforms:
                 - name: "my-transform"
                   transform:
                    type: "path"
                    path: "record"
               """)

      # Then update it
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               transforms:
                 - name: "my-transform"
                   transform:
                     type: "path"
                     path: "record.id"
               """)

      assert [transform] = Repo.all(Transform)
      assert transform.name == "my-transform"
      assert transform.type == "path"
      assert transform.transform.path == "record.id"
    end

    test "creates multiple transforms" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               transforms:
                 - name: "transform-1"
                   transform:
                     type: "path"
                     path: "record"
                 - name: "transform-2"
                   transform:
                     type: "path"
                     path: "record.status"
               """)

      assert transforms = Repo.all(Transform)
      assert length(transforms) == 2

      transform1 = Enum.find(transforms, &(&1.name == "transform-1"))
      assert transform1.type == "path"
      assert transform1.transform.path == "record"

      transform2 = Enum.find(transforms, &(&1.name == "transform-2"))
      assert transform2.type == "path"
      assert transform2.transform.path == "record.status"
    end

    test "handles invalid transform type" do
      assert_raise RuntimeError, ~r/Error creating transform/, fn ->
        YamlLoader.apply_from_yml!("""
        account:
          name: "Configured by Sequin"

        transforms:
          - name: "invalid-transform"
            transform:
              type: "invalid_type"
        """)
      end
    end

    test "creates a function transform" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               account:
                 name: "Configured by Sequin"

               transforms:
                 - name: "id-action-functional"
                   description: "Record ID and action"
                   transform:
                     type: "function"
                     code: |-
                        def transform(action, record, changes, metadata) do
                          %{
                            id: record["id"],
                            action: action
                          }
                        end
               """)

      assert [transform] = Repo.all(Transform)
      assert transform.name == "id-action-functional"
      assert transform.type == "function"
      assert transform.description == "Record ID and action"

      assert transform.transform.code ==
               String.trim("""
               def transform(action, record, changes, metadata) do
                 %{
                   id: record["id"],
                   action: action
                 }
               end
               """)
    end
  end

  describe "sinks" do
    def account_db_and_sequence_yml do
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
               #{account_db_and_sequence_yml()}

               transforms:
                 - name: "record_only"
                   transform:
                     type: "path"
                     path: "record"

               http_endpoints:
                 - name: "sequin-playground-http"
                   url: "https://api.example.com/webhook"

               sinks:
                 - name: "sequin-playground-webhook"
                   database: "test-db"
                   table: "Characters"
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   transform: "record_only"
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, [:sequence, :transform])

      assert consumer.name == "sequin-playground-webhook"
      assert consumer.sequence.name == "test-db.public.Characters"
      assert consumer.transform.name == "record_only"

      assert consumer.sequence_filter == %SequenceFilter{
               actions: [:insert, :update, :delete],
               column_filters: [],
               group_column_attnums: [1]
             }
    end

    test "creates sink consumer with filters" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_db_and_sequence_yml()}

               http_endpoints:
                 - name: "sequin-playground-http"
                   url: "https://api.example.com/webhook"

               sinks:
                 - name: "sequin-playground-webhook"
                   database: "test-db"
                   table: "Characters"
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   filters:
                     - column_name: "house"
                       operator: "="
                       comparison_value: "Stark"
                     - column_name: "name"
                       operator: "is not null"
                     - column_name: "metadata"
                       field_path: "rank.title"
                       operator: "="
                       comparison_value: "Lord"
                       field_type: "string"
                     - column_name: "is_active"
                       operator: "="
                       comparison_value: true
                     - column_name: "planet"
                       operator: "in"
                       comparison_value:
                        - "Alderaan"
                        - "Tatooine"
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      assert consumer.name == "sequin-playground-webhook"

      filters = consumer.sequence_filter.column_filters
      assert length(filters) == 5

      # House filter
      house_filter = Enum.find(filters, &(&1.value.value == "Stark"))
      assert house_filter.operator == :==
      assert house_filter.is_jsonb == false

      # Name filter
      name_filter = Enum.find(filters, &(&1.operator == :not_null))
      assert name_filter.value == %NullValue{value: nil}
      assert name_filter.is_jsonb == false

      # Metadata filter
      metadata_filter = Enum.find(filters, &(&1.jsonb_path == "rank.title"))
      assert metadata_filter.operator == :==
      assert metadata_filter.is_jsonb == true
      assert metadata_filter.value == %StringValue{value: "Lord"}

      # Is active filter
      active_filter = Enum.find(filters, &(&1.value.value == true))
      assert active_filter.operator == :==
      assert active_filter.is_jsonb == false

      # Planet filter
      planet_filter = Enum.find(filters, &(&1.value.value == ["Alderaan", "Tatooine"]))
      assert planet_filter.operator == :in
      assert planet_filter.is_jsonb == false
    end

    test "applying yml twice creates no duplicates" do
      yaml = """
      #{account_db_and_sequence_yml()}

      http_endpoints:
        - name: "sequin-playground-http"
          url: "https://api.example.com/webhook"

      sinks:
        - name: "sequin-playground-webhook"
          database: "test-db"
          table: "Characters"
          destination:
            type: "webhook"
            http_endpoint: "sequin-playground-http"
        - name: "sequin-playground-kafka"
          database: "test-db"
          table: "Characters"
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
      #{account_db_and_sequence_yml()}

      http_endpoints:
        - name: "sequin-playground-http"
          url: "https://api.example.com/webhook"

      sinks:
        - name: "sequin-playground-webhook"
          database: "test-db"
          table: "Characters"
          destination:
            type: "webhook"
            http_endpoint: "sequin-playground-http"
      """

      assert :ok = YamlLoader.apply_from_yml!(create_yaml)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = SinkConsumer.preload_http_endpoint(consumer)

      assert consumer.name == "sequin-playground-webhook"
      assert consumer.sink.http_endpoint.name == "sequin-playground-http"

      update_yaml = """
      #{account_db_and_sequence_yml()}

      http_endpoints:
        - name: "new-http-endpoint"
          url: "https://api.example.com/webhook"

      sinks:
        - name: "sequin-playground-webhook"
          database: "test-db"
          table: "Characters"
          destination:
            type: "webhook"
            http_endpoint: "new-http-endpoint"
      """

      # Update with different filters
      assert :ok = YamlLoader.apply_from_yml!(update_yaml)

      assert [updated_consumer] = Repo.all(SinkConsumer)
      updated_consumer = SinkConsumer.preload_http_endpoint(updated_consumer)

      assert updated_consumer.name == "sequin-playground-webhook"
      assert updated_consumer.sink.http_endpoint.name == "new-http-endpoint"
    end

    test "creates sequin stream consumer" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_db_and_sequence_yml()}

               sinks:
                 - name: "sequin-playground-consumer"
                   database: "test-db"
                   table: "Characters"
                   group_column_names: ["house"]
                   destination:
                     type: "sequin_stream"
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, :sequence)

      assert consumer.name == "sequin-playground-consumer"
      assert consumer.sequence.name == "test-db.public.Characters"
      assert consumer.legacy_transform == :none

      assert consumer.sequence_filter == %SequenceFilter{
               actions: [:insert, :update, :delete],
               column_filters: [],
               group_column_attnums: [Character.column_attnum("house")]
             }

      assert %SequinStreamSink{} = consumer.sink
    end

    test "creates kafka sink consumer" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_db_and_sequence_yml()}

               sinks:
                 - name: "kafka-consumer"
                   database: "test-db"
                   table: "Characters"
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
      consumer = Repo.preload(consumer, :sequence)

      assert consumer.name == "kafka-consumer"
      assert consumer.sequence.name == "test-db.public.Characters"

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
               #{account_db_and_sequence_yml()}

               sinks:
                 - name: "sqs-consumer"
                   database: "test-db"
                   table: "Characters"
                   destination:
                     type: "sqs"
                     queue_url: "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue.fifo"
                     access_key_id: "AKIAXXXXXXXXXXXXXXXX"
                     secret_access_key: "secret123"
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, :sequence)

      assert consumer.name == "sqs-consumer"
      assert consumer.sequence.name == "test-db.public.Characters"

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
               #{account_db_and_sequence_yml()}

               sinks:
                 - name: "sns-consumer"
                   database: "test-db"
                   table: "Characters"
                   destination:
                     type: "sns"
                     topic_arn: "arn:aws:sns:us-west-2:123456789012:MyTopic"
                     access_key_id: "AKIAXXXXXXXXXXXXXXXX"
                     secret_access_key: "secret123"
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, :sequence)

      assert consumer.name == "sns-consumer"
      assert consumer.sequence.name == "test-db.public.Characters"

      assert %SnsSink{
               type: :sns,
               topic_arn: "arn:aws:sns:us-west-2:123456789012:MyTopic",
               region: "us-west-2",
               access_key_id: "AKIAXXXXXXXXXXXXXXXX",
               secret_access_key: "secret123"
             } = consumer.sink
    end

    test "creates redis sink consumer" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_db_and_sequence_yml()}

               sinks:
                 - name: "redis-consumer"
                   database: "test-db"
                   table: "Characters"
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
      consumer = Repo.preload(consumer, :sequence)

      assert consumer.name == "redis-consumer"
      assert consumer.sequence.name == "test-db.public.Characters"

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
               #{account_db_and_sequence_yml()}

               sinks:
                 - name: "pubsub-consumer"
                   database: "test-db"
                   table: "Characters"
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
      consumer = Repo.preload(consumer, :sequence)

      assert consumer.name == "pubsub-consumer"
      assert consumer.sequence.name == "test-db.public.Characters"

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
               #{account_db_and_sequence_yml()}

               sinks:
                 - name: "elasticsearch-consumer"
                   database: "test-db"
                   table: "Characters"
                   destination:
                     type: "elasticsearch"
                     endpoint_url: "https://elasticsearch.example.com"
                     index_name: "test-index"
                     auth_type: "api_key"
                     auth_value: "sensitive-api-key"
                     batch_size: 100
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, :sequence)

      assert consumer.name == "elasticsearch-consumer"
      assert consumer.sequence.name == "test-db.public.Characters"

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
               #{account_db_and_sequence_yml()}

               sinks:
                 - name: "redis-string-consumer"
                   database: "test-db"
                   table: "Characters"
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
      consumer = Repo.preload(consumer, :sequence)

      assert consumer.name == "redis-string-consumer"
      assert consumer.sequence.name == "test-db.public.Characters"

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
      #{account_db_and_sequence_yml()}

      http_endpoints:
        - name: "sequin-playground-http"
          url: "https://api.example.com/webhook"

      sinks:
        - name: "sequin-playground-webhook"
          database: "test-db"
          table: "Characters"
          destination:
            type: "webhook"
            http_endpoint: "sequin-playground-http"
      """

      assert :ok = YamlLoader.apply_from_yml!(initial_yaml)

      # Verify initial state
      assert [initial_sequence] = Repo.all(Sequence)
      assert [initial_consumer] = Repo.all(SinkConsumer)
      assert [db] = Repo.all(PostgresDatabase)
      assert initial_consumer.sequence_id == initial_sequence.id

      # Remove the sink
      Consumers.delete_sink_consumer(initial_consumer)

      # Verify sink was removed but sequence remains
      assert [_sequence] = Repo.all(Sequence)
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

      # Verify final state - should reuse existing sequence
      assert [final_sequence] = Repo.all(Sequence)
      assert [final_consumer] = Repo.all(SinkConsumer)
      assert final_sequence.id == initial_sequence.id
      assert final_consumer.sequence_id == final_sequence.id
    end

    test "creates multiple sinks using YAML anchors" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_db_and_sequence_yml()}

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
                   table: public.Characters
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, :sequence)

      assert consumer.name == "gcp-events-characters"
      assert consumer.status == :active
      assert consumer.legacy_transform == :none
      assert consumer.sequence.name == "test-db.public.Characters"

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
               #{account_db_and_sequence_yml()}

               http_endpoints:
                - name: "sequin-playground-http"
                  url: "https://api.example.com/webhook"

               sinks:
                 - name: "custom-actions-sink"
                   database: "test-db"
                   table: "Characters"
                   actions:
                     - insert
                     - delete
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   timestamp_format: "unix_microsecond"
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, :sequence)

      assert consumer.name == "custom-actions-sink"
      assert consumer.sequence.name == "test-db.public.Characters"

      assert consumer.sequence_filter == %SequenceFilter{
               actions: [:insert, :delete],
               column_filters: [],
               group_column_attnums: [1]
             }

      assert consumer.timestamp_format == :unix_microsecond
    end

    test "creates multiple sinks with different names using YAML anchors" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_db_and_sequence_yml()}

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
                   table: public.Characters
                 - <<: *sink_template
                   name: gcp-events-characters-2
                   table: public.Characters
               """)

      assert consumers = Repo.all(SinkConsumer)
      assert length(consumers) == 2

      consumer_names = Enum.map(consumers, & &1.name)
      assert "gcp-events-characters-1" in consumer_names
      assert "gcp-events-characters-2" in consumer_names

      for consumer <- consumers do
        consumer = Repo.preload(consumer, :sequence)
        assert consumer.status == :active
        assert consumer.legacy_transform == :none
        assert consumer.sequence.name == "test-db.public.Characters"

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
               #{account_db_and_sequence_yml()}

               transforms:
                 - name: "my-transform"
                   transform:
                    type: "path"
                    path: "record"

               http_endpoints:
                 - name: "sequin-playground-http"
                   url: "https://api.example.com/webhook"

               sinks:
                 - name: "sequin-playground-webhook"
                   database: "test-db"
                   table: "Characters"
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   transform: "my-transform"
               """)

      assert [transform] = Repo.all(Transform)
      assert transform.name == "my-transform"
      assert transform.type == "path"
      assert transform.transform.path == "record"

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, :sequence)

      assert consumer.name == "sequin-playground-webhook"
      assert consumer.sequence.name == "test-db.public.Characters"
      assert consumer.transform_id == transform.id
    end

    test "creates webhook subscription with function transform" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_db_and_sequence_yml()}

               transforms:
                 - name: "id-action-transform"
                   description: "Extract ID and action"
                   transform:
                     type: "function"
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
                   table: "Characters"
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   transform: "id-action-transform"
               """)

      assert [transform] = Repo.all(Transform)
      assert transform.name == "id-action-transform"
      assert transform.type == "function"
      assert transform.description == "Extract ID and action"

      assert transform.transform.code ==
               String.trim("""
               def transform(action, record, changes, metadata) do
                 %{
                   id: record["id"],
                   action: action
                 }
               end
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, :sequence)

      assert consumer.name == "sequin-playground-webhook"
      assert consumer.sequence.name == "test-db.public.Characters"
      assert consumer.transform_id == transform.id
    end

    test "creates sink with no transform" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_db_and_sequence_yml()}

               http_endpoints:
                 - name: "sequin-playground-http"
                   url: "https://api.example.com/webhook"

               sinks:
                 - name: "sequin-playground-webhook"
                   database: "test-db"
                   table: "Characters"
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   transform: "none"
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, :sequence)

      assert consumer.name == "sequin-playground-webhook"
      assert consumer.sequence.name == "test-db.public.Characters"
      assert consumer.transform_id == nil
    end

    test "raises error when referenced transform not found" do
      assert_raise RuntimeError, ~r/Transform 'missing-transform' not found/, fn ->
        YamlLoader.apply_from_yml!("""
        #{account_db_and_sequence_yml()}

        http_endpoints:
          - name: "sequin-playground-http"
            url: "https://api.example.com/webhook"

        sinks:
          - name: "sequin-playground-webhook"
            database: "test-db"
            table: "Characters"
            destination:
              type: "webhook"
              http_endpoint: "sequin-playground-http"
            transform: "missing-transform"
        """)
      end
    end

    test "creates webhook subscription with routing function reference" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_db_and_sequence_yml()}

               transforms:
                 - name: "my-routing"
                   transform:
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
                   table: "Characters"
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   routing: "my-routing"
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, [:sequence, :routing])

      assert consumer.name == "sequin-playground-webhook"
      assert consumer.sequence.name == "test-db.public.Characters"

      # Check routing function reference was used
      assert consumer.routing
      assert consumer.routing.name == "my-routing"
      assert consumer.routing.transform.type == :routing
      assert consumer.routing.transform.sink_type == :http_push
      assert consumer.routing.transform.code =~ "def route(action, record, changes, metadata)"
      assert consumer.routing.transform.code =~ "/custom/"
    end

    test "creates webhook subscription with new yaml names" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_db_and_sequence_yml()}

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
                   table: "Characters"
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   routing: "my-routing"
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, [:sequence, :routing])

      assert consumer.routing.name == "my-routing"
    end

    test "creates webhook subscription with FLAT YAML" do
      assert :ok =
               YamlLoader.apply_from_yml!("""
               #{account_db_and_sequence_yml()}

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
                   table: "Characters"
                   destination:
                     type: "webhook"
                     http_endpoint: "sequin-playground-http"
                   routing: "my-routing"
               """)

      assert [consumer] = Repo.all(SinkConsumer)
      consumer = Repo.preload(consumer, [:sequence, :routing])

      assert consumer.routing.name == "my-routing"
    end

    test "errors when routing transform doesn't exist" do
      assert_raise RuntimeError, ~r/[Tt]ransform 'non-existent-routing' not found/, fn ->
        YamlLoader.apply_from_yml!("""
        #{account_db_and_sequence_yml()}

        http_endpoints:
          - name: "sequin-playground-http"
            url: "https://api.example.com/webhook"

        sinks:
          - name: "sequin-playground-webhook"
            database: "test-db"
            table: "Characters"
            destination:
              type: "webhook"
              http_endpoint: "sequin-playground-http"
            routing: "non-existent-routing"
        """)
      end
    end

    test "errors when transform referenced for routing is not a routing transform" do
      assert_raise RuntimeError, "`routing` must reference a transform with type `routing`", fn ->
        YamlLoader.apply_from_yml!("""
        #{account_db_and_sequence_yml()}

        transforms:
          - name: "regular-transform"
            transform:
              type: "path"
              path: "record"

        http_endpoints:
          - name: "sequin-playground-http"
            url: "https://api.example.com/webhook"

        sinks:
          - name: "sequin-playground-webhook"
            database: "test-db"
            table: "Characters"
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

      assert {:ok, {:ok, _resources}} = result

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
