defmodule Sequin.DatabasesRuntime.SlotSupervisor do
  @moduledoc false
  use Supervisor

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.DatabasesRuntime.SlotProcessor.MessageHandler
  alias Sequin.Replication.PostgresReplicationSlot

  def start_link(opts) do
    %PostgresReplicationSlot{} = pg_replication = Keyword.fetch!(opts, :pg_replication)
    Supervisor.start_link(__MODULE__, opts, name: {:via, :syn, {:replication, pg_replication.id}})
  end

  @impl Supervisor
  def init(opts) do
    Supervisor.init(children(opts), strategy: :one_for_all)
  end

  defp children(opts) do
    %PostgresReplicationSlot{} = pg_replication = Keyword.fetch!(opts, :pg_replication)
    slot_message_store_opts = Keyword.get(opts, :slot_message_store_opts, [])

    [
      slot_processor_child_spec(pg_replication, opts)
      | Enum.map(pg_replication.sink_consumers, &slot_message_store_child_spec(&1, slot_message_store_opts))
    ]
  end

  defp slot_processor_child_spec(%PostgresReplicationSlot{} = pg_replication, opts) do
    default_opts = [
      id: pg_replication.id,
      slot_name: pg_replication.slot_name,
      publication: pg_replication.publication_name,
      postgres_database: pg_replication.postgres_database,
      message_handler_ctx: MessageHandler.context(pg_replication),
      message_handler_module: MessageHandler,
      connection: PostgresDatabase.to_postgrex_opts(pg_replication.postgres_database),
      ipv6: pg_replication.postgres_database.ipv6
    ]

    opts = Keyword.merge(default_opts, opts)
    {Sequin.DatabasesRuntime.SlotProcessor, opts}
  end

  defp slot_message_store_child_spec(%SinkConsumer{} = sink_consumer, opts) do
    opts = Keyword.put(opts, :consumer_id, sink_consumer.id)
    {Sequin.DatabasesRuntime.SlotMessageStore, opts}
  end
end
