defmodule Sequin.Runtime.SlotProcessorSupervisor do
  @moduledoc """
  A supervisor for the slot processor and slot message handlers.
  """

  use Supervisor

  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Runtime.MessageHandler
  alias Sequin.Runtime.SlotMessageHandler
  alias Sequin.Runtime.SlotProcessorServer

  def child_spec(opts) do
    %PostgresReplicationSlot{} = slot = Keyword.fetch!(opts, :replication_slot)

    spec = %{
      id: via_tuple(slot.id),
      start: {__MODULE__, :start_link, [opts]}
    }

    Supervisor.child_spec(spec, restart: :temporary)
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
    message_handler_module = Keyword.get_lazy(opts, :message_handler_module, &default_message_handler_module/0)

    default_opts =
      [
        id: slot.id,
        slot_name: slot.slot_name,
        publication: slot.publication_name,
        postgres_database: slot.postgres_database,
        replication_slot: slot,
        message_handler_ctx_fn: &MessageHandler.context/1,
        message_handler_module: message_handler_module,
        connection: PostgresDatabase.to_postgrex_opts(slot.postgres_database),
        ipv6: slot.postgres_database.ipv6
      ]

    slot_opts = Keyword.merge(default_opts, opts)

    message_handlers =
      Enum.map(0..(slot.partition_count - 1), fn idx ->
        opts = [
          replication_slot_id: slot.id,
          processor_idx: idx,
          test_pid: opts[:test_pid]
        ]

        SlotMessageHandler.child_spec(opts)
      end)

    children = [
      {SlotProcessorServer, slot_opts}
      | message_handlers
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp default_message_handler_module do
    Application.get_env(:sequin, :message_handler_module)
  end
end
