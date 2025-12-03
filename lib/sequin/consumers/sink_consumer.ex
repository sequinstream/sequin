defmodule Sequin.Consumers.SinkConsumer do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query
  import PolymorphicEmbed

  alias __MODULE__
  alias Sequin.Accounts.Account
  alias Sequin.Consumers
  alias Sequin.Consumers.AzureEventHubSink
  alias Sequin.Consumers.Backfill
  alias Sequin.Consumers.ElasticsearchSink
  alias Sequin.Consumers.Function
  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Consumers.HttpPushSink
  alias Sequin.Consumers.KafkaSink
  alias Sequin.Consumers.KinesisSink
  alias Sequin.Consumers.MeilisearchSink
  alias Sequin.Consumers.NatsSink
  alias Sequin.Consumers.RabbitMqSink
  alias Sequin.Consumers.RedisStreamSink
  alias Sequin.Consumers.RedisStringSink
  alias Sequin.Consumers.S2Sink
  alias Sequin.Consumers.SequinStreamSink
  alias Sequin.Consumers.SnsSink
  alias Sequin.Consumers.Source
  alias Sequin.Consumers.SourceTable
  alias Sequin.Consumers.SqsSink
  alias Sequin.Consumers.TypesenseSink
  alias Sequin.Replication.PostgresReplicationSlot

  @type id :: String.t()
  @type ack_id :: String.t()
  @type not_visible_until :: DateTime.t()

  @types [
    :http_push,
    :sqs,
    :kinesis,
    :s2,
    :redis_stream,
    :redis_string,
    :kafka,
    :sequin_stream,
    :gcp_pubsub,
    :nats,
    :rabbitmq,
    :azure_event_hub,
    :typesense,
    :meilisearch,
    :sns,
    :elasticsearch
  ]

  # This is a module attribute to compile the types into the schema
  def types, do: @types

  @derive {Jason.Encoder,
           only: [
             :account_id,
             :ack_wait_ms,
             :id,
             :inserted_at,
             :max_ack_pending,
             :max_deliver,
             :max_waiting,
             :message_kind,
             :name,
             :updated_at,
             :status,
             :health,
             :max_memory_mb,
             :legacy_transform,
             :timestamp_format,
             :batch_timeout_ms,
             :max_retry_count,
             :load_shedding_policy,
             :message_grouping
           ]}
  typed_schema "sink_consumers" do
    field :name, :string
    field :backfill_completed_at, :utc_datetime_usec
    field :ack_wait_ms, :integer, default: 30_000
    field :max_ack_pending, :integer, default: 10_000
    field :max_deliver, :integer
    field :max_waiting, :integer, default: 20
    field :max_retry_count, :integer, default: nil
    field :message_kind, Ecto.Enum, values: [:event, :record], default: :event
    field :status, Ecto.Enum, values: [:active, :disabled, :paused], default: :active
    field :seq, :integer, read_after_writes: true
    field :batch_size, :integer, default: 1
    field :batch_timeout_ms, :integer, default: nil
    field :annotations, :map, default: %{}
    field :max_memory_mb, :integer, default: 128
    field :partition_count, :integer, default: 1
    field :legacy_transform, Ecto.Enum, values: [:none, :record_only], default: :none
    field :timestamp_format, Ecto.Enum, values: [:iso8601, :unix_microsecond], default: :iso8601
    field :load_shedding_policy, Ecto.Enum, values: [:pause_on_full, :discard_on_full], default: :pause_on_full
    field :message_grouping, :boolean, default: true

    field :type, Ecto.Enum, values: @types, read_after_writes: true

    field :health, :map, virtual: true

    has_many :active_backfills, Backfill, where: [state: {:in, [:active, :paused]}]

    field :actions, {:array, Ecto.Enum}, values: [:insert, :update, :delete]
    embeds_one :source, Source, on_replace: :delete
    embeds_many :source_tables, SourceTable, on_replace: :delete

    belongs_to :account, Account
    belongs_to :replication_slot, PostgresReplicationSlot
    has_one :postgres_database, through: [:replication_slot, :postgres_database]

    belongs_to :transform, Function
    belongs_to :routing, Function
    belongs_to :filter, Function
    belongs_to :enrichment, Function

    polymorphic_embeds_one(:sink,
      types: [
        http_push: HttpPushSink,
        sqs: SqsSink,
        kinesis: KinesisSink,
        s2: S2Sink,
        sns: SnsSink,
        redis_stream: RedisStreamSink,
        redis_string: RedisStringSink,
        kafka: KafkaSink,
        sequin_stream: SequinStreamSink,
        gcp_pubsub: GcpPubsubSink,
        nats: NatsSink,
        rabbitmq: RabbitMqSink,
        azure_event_hub: AzureEventHubSink,
        typesense: TypesenseSink,
        meilisearch: MeilisearchSink,
        elasticsearch: ElasticsearchSink
      ],
      on_replace: :update,
      type_field_name: :type
    )

    timestamps()
  end

  def create_changeset(consumer, attrs) do
    consumer
    |> cast(attrs, [
      :replication_slot_id,
      :status,
      :message_kind,
      :max_memory_mb,
      :transform_id,
      :routing_id,
      :filter_id,
      :enrichment_id
    ])
    |> changeset(attrs)
    |> unique_constraint([:account_id, :name], error_key: :name)
    |> check_constraint(:batch_size,
      name: "ensure_batch_size_one",
      message: "batch_size must be 1 when batch is false for webhook sinks"
    )
    |> Sequin.Changeset.validate_name()
  end

  def update_changeset(consumer, attrs) do
    consumer
    |> changeset(attrs)
    |> check_constraint(:batch_size,
      name: "ensure_batch_size_one",
      message: "batch_size must be 1 when batch is false for webhook sinks"
    )
  end

  def changeset(consumer, attrs) do
    attrs = put_sink_routing_mode(attrs)

    consumer
    |> cast(attrs, [
      :name,
      :actions,
      :batch_size,
      :ack_wait_ms,
      :max_waiting,
      :max_ack_pending,
      :max_deliver,
      :max_retry_count,
      :backfill_completed_at,
      :status,
      :annotations,
      :max_memory_mb,
      :partition_count,
      :legacy_transform,
      :transform_id,
      :routing_id,
      :filter_id,
      :enrichment_id,
      :timestamp_format,
      :batch_timeout_ms,
      :load_shedding_policy,
      :message_grouping
    ])
    |> cast_polymorphic_embed(:sink, required: true)
    |> cast_embed(:source, required: true)
    |> cast_embed(:source_tables)
    |> put_defaults()
    |> validate_message_grouping()
    |> validate_enrichment()
    |> validate_required([:name, :status, :replication_slot_id, :batch_size])
    |> validate_number(:ack_wait_ms, greater_than_or_equal_to: 500)
    |> validate_number(:batch_size, greater_than: 0)
    |> validate_number(:batch_size, less_than_or_equal_to: 1_000)
    |> validate_number(:batch_timeout_ms, greater_than: 0)
    |> validate_number(:max_memory_mb, greater_than_or_equal_to: 128)
    |> validate_number(:partition_count, greater_than_or_equal_to: 1)
    |> validate_number(:max_retry_count, greater_than: 0)
    |> validate_inclusion(:legacy_transform, [:none, :record_only])
    |> validate_routing(attrs)
    |> foreign_key_constraint(:transform_id)
    |> foreign_key_constraint(:routing_id)
    |> foreign_key_constraint(:filter_id)
    |> foreign_key_constraint(:enrichment_id)
    |> Sequin.Changeset.annotations_check_constraint()
  end

  defp validate_message_grouping(changeset) do
    message_grouping = get_field(changeset, :message_grouping)
    source_tables = get_field(changeset, :source_tables) || []

    if message_grouping == false do
      invalid_tables =
        Enum.filter(source_tables, fn table ->
          table.group_column_attnums && length(table.group_column_attnums) > 0
        end)

      if length(invalid_tables) > 0 do
        table_names =
          Enum.map(invalid_tables, fn table ->
            table.table_name || "[oid: #{table.table_oid}]"
          end)

        add_error(
          changeset,
          :message_grouping,
          "Cannot specify group columns when message_grouping is disabled. " <>
            "Found group columns on: #{Enum.join(table_names, ", ")}. " <>
            "For more information, see: https://sequinstream.com/docs/reference/sequin-yaml#message-grouping"
        )
      else
        changeset
      end
    else
      changeset
    end
  end

  defp validate_enrichment(changeset) do
    source = get_field(changeset, :source)
    enrichment_id = get_field(changeset, :enrichment_id)

    has_enrichment? = not is_nil(enrichment_id)

    has_single_table? =
      not is_nil(source) and is_list(source.include_table_oids) and length(source.include_table_oids) == 1

    if has_enrichment? and not has_single_table? do
      add_error(
        changeset,
        :enrichment_id,
        "Enrichment is not supported for multiple tables. To use enrichment, please specify a single included table in the source."
      )
    else
      changeset
    end
  end

  defp validate_routing(cs, attrs) do
    case {attrs["routing_mode"] || attrs[:routing_mode], fetch_field(cs, :routing_id)} do
      {nil, {:data, _}} ->
        # Both unchanged, this is fine
        cs

      {"static", {_, _}} ->
        validate_inclusion(cs, :routing_id, [nil])

      {"dynamic", {_, _}} ->
        validate_required(cs, [:routing_id])

      no_match ->
        add_error(cs, :routing_id, "unknown routing mode: #{inspect(no_match)}")
    end
  end

  defp put_sink_routing_mode(attrs) do
    routing_mode = attrs["routing_mode"] || attrs[:routing_mode]
    sink = attrs["sink"] || attrs[:sink]

    if not is_nil(routing_mode) and not is_nil(sink) do
      binary_keys? = Enum.any?(sink, fn {key, _} -> is_binary(key) end)

      if binary_keys? do
        put_in(attrs, ["sink", "routing_mode"], routing_mode)
      else
        put_in(attrs, [:sink, :routing_mode], routing_mode)
      end
    else
      attrs
    end
  end

  defp put_defaults(changeset) do
    message_grouping =
      case get_field(changeset, :message_grouping) do
        nil -> true
        bool -> bool
      end

    changeset
    |> put_change(:batch_size, get_field(changeset, :batch_size) || default_batch_size(get_field(changeset, :type)))
    |> put_change(:ack_wait_ms, get_field(changeset, :ack_wait_ms) || 30_000)
    |> put_change(:max_waiting, get_field(changeset, :max_waiting) || 20)
    |> put_change(:max_ack_pending, get_field(changeset, :max_ack_pending) || 10_000)
    |> put_change(:max_memory_mb, get_field(changeset, :max_memory_mb) || 128)
    |> put_change(:partition_count, get_field(changeset, :partition_count) || 1)
    |> put_change(:legacy_transform, get_field(changeset, :legacy_transform) || :none)
    |> put_change(:message_kind, get_field(changeset, :message_kind) || :event)
    |> put_change(:message_grouping, message_grouping)
  end

  defp default_batch_size(type) when is_atom(type) do
    case type do
      :sqs -> 10
      :sns -> 10
      :kinesis -> 100
      :s2 -> 10
      :kafka -> 200
      :redis_stream -> 50
      :gcp_pubsub -> 1
      :azure_event_hub -> 10
      :redis_string -> 10
      _ -> 1
    end
  end

  def where_account_id(query \\ base_query(), account_id) do
    from([consumer: c] in query, where: c.account_id == ^account_id)
  end

  def where_replication_slot_id(query \\ base_query(), replication_slot_id) do
    from([consumer: c] in query, where: c.replication_slot_id == ^replication_slot_id)
  end

  def where_http_endpoint_id(query \\ base_query(), http_endpoint_id) do
    query = where_type(query, :http_push)

    from([consumer: c] in query, where: fragment("?->>'http_endpoint_id' = ?", c.sink, ^http_endpoint_id))
  end

  def where_transform_id(query \\ base_query(), transform_id) do
    from([consumer: c] in query, where: c.transform_id == ^transform_id)
  end

  def where_any_function_id(query \\ base_query(), function_id) do
    from([consumer: c] in query,
      where:
        c.transform_id == ^function_id or c.routing_id == ^function_id or c.filter_id == ^function_id or
          c.enrichment_id == ^function_id
    )
  end

  def where_type(query \\ base_query(), type) do
    from([consumer: c] in query, where: c.type == ^type)
  end

  def where_id(query \\ base_query(), id) do
    from([consumer: c] in query, where: c.id == ^id)
  end

  def where_id_in(query \\ base_query(), ids) do
    from([consumer: c] in query, where: c.id in ^ids)
  end

  def where_active_backfill(query \\ base_query()) do
    from([consumer: c] in query,
      inner_join: b in Backfill,
      on: b.sink_consumer_id == c.id and b.state == :active
    )
  end

  def where_name(query \\ base_query(), name) do
    from([consumer: c] in query, where: c.name == ^name)
  end

  def where_id_or_name(query \\ base_query(), id_or_name) do
    if Sequin.String.sequin_uuid?(id_or_name) do
      where_id(query, id_or_name)
    else
      where_name(query, id_or_name)
    end
  end

  def where_kind(query \\ base_query(), kind) do
    from([consumer: c] in query, where: c.kind == ^kind)
  end

  def where_status(query \\ base_query(), status) do
    from([consumer: c] in query, where: c.status == ^status)
  end

  def where_status_not(query \\ base_query(), status) do
    from([consumer: c] in query, where: c.status != ^status)
  end

  def join_postgres_database(query \\ base_query()) do
    from([consumer: c] in query,
      join: postgres_database in assoc(c, :postgres_database),
      as: :database
    )
  end

  defp base_query(query \\ __MODULE__) do
    from(c in query, as: :consumer)
  end

  @backfill_completed_at_threshold to_timeout(minute: 5)
  def should_delete_acked_messages?(consumer, now \\ DateTime.utc_now())

  def should_delete_acked_messages?(%SinkConsumer{backfill_completed_at: nil}, _now), do: false

  def should_delete_acked_messages?(%SinkConsumer{backfill_completed_at: backfill_completed_at}, now) do
    backfill_completed_at
    |> DateTime.add(@backfill_completed_at_threshold, :millisecond)
    |> DateTime.compare(now) == :lt
  end

  def preload_http_endpoint!(%HttpPushSink{http_endpoint: nil} = sink) do
    http_endpoint = Consumers.get_http_endpoint!(sink.http_endpoint_id)
    %{sink | http_endpoint: http_endpoint}
  end

  def preload_http_endpoint!(%SinkConsumer{sink: %HttpPushSink{http_endpoint: nil}} = consumer) do
    %{consumer | sink: preload_http_endpoint!(consumer.sink)}
  end

  def preload_http_endpoint!(consumer), do: consumer

  def preload_cached_http_endpoint(%HttpPushSink{http_endpoint: nil} = sink) do
    with {:ok, http_endpoint} <- Consumers.get_cached_http_endpoint(sink.http_endpoint_id) do
      {:ok, %{sink | http_endpoint: http_endpoint}}
    end
  end

  def preload_cached_http_endpoint(%SinkConsumer{sink: %HttpPushSink{http_endpoint: nil}} = consumer) do
    with {:ok, sink} <- preload_cached_http_endpoint(consumer.sink) do
      {:ok, %{consumer | sink: sink}}
    end
  end

  def preload_cached_http_endpoint(consumer), do: {:ok, consumer}
end
