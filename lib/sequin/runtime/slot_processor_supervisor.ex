defmodule Sequin.Runtime.SlotProcessorSupervisor do
  @moduledoc """
  A supervisor for the slot processor and slot message handlers.
  """

  use Supervisor

  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo
  alias Sequin.Runtime.MessageHandler
  alias Sequin.Runtime.SlotMessageHandler
  alias Sequin.Runtime.SlotProcessor

  def child_tuple(%PostgresReplicationSlot{} = slot, opts \\ []) do
    opts = Keyword.put(opts, :replication_slot, slot)

    {__MODULE__, opts}
  end

  def start_link(opts) do
    replication_slot = Keyword.fetch!(opts, :replication_slot)
    Supervisor.start_link(__MODULE__, opts, name: via_tuple(replication_slot.id))
  end

  def via_tuple(id) do
    {:via, :syn, {:replication, {__MODULE__, id}}}
  end

  def init(opts) do
    {slot, opts} = Keyword.pop!(opts, :replication_slot)
    slot = Repo.preload(slot, :postgres_database)

    default_opts =
      [
        id: slot.id,
        slot_name: slot.slot_name,
        publication: slot.publication_name,
        postgres_database: slot.postgres_database,
        replication_slot: slot,
        message_handler_ctx: MessageHandler.context(slot),
        message_handler_module: SlotMessageHandler,
        connection: PostgresDatabase.to_postgrex_opts(slot.postgres_database),
        ipv6: slot.postgres_database.ipv6
      ]

    slot_opts = Keyword.merge(default_opts, opts)

    message_handlers =
      Enum.map(0..(slot.processor_count - 1), fn idx ->
        opts = [
          replication_slot_id: slot.id,
          processor_idx: idx
        ]

        SlotMessageHandler.child_spec(opts)
      end)

    children = [
      {SlotProcessor, slot_opts}
      | message_handlers
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
