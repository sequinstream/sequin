defmodule Sequin.Functions.TestMessages do
  @moduledoc """
  Manages :ets table for storing messages for testing transforms.

  Users use these messages to see the before/after when writing ie. PathFunctions in the console.
  """

  alias Sequin.Functions.TestMessagesRegistry

  require Logger

  def registry, do: TestMessagesRegistry

  @type database_id :: String.t()
  @type table_oid :: integer()
  @type consumer_message :: Sequin.Consumers.ConsumerRecord.t() | Sequin.Consumers.ConsumerEvent.t()

  def max_message_count, do: 10

  @doc """
  Creates a new ETS table for storing test messages.
  """
  @spec create_ets_table() :: :ets.tid()
  def create_ets_table do
    :ets.new(:test_messages, [:named_table, :set, :public, read_concurrency: true, write_concurrency: true])
  rescue
    ArgumentError ->
      :ok
  end

  @doc """
  Registers that a process needs test messages for a sequence.
  """
  @spec register_needs_messages(database_id()) :: {:ok, any()} | {:error, any()}
  def register_needs_messages(database_id) do
    Logger.info("[TestMessages] Registering needs messages for #{database_id}")
    Registry.register(TestMessagesRegistry, database_id, :ok)
  end

  @spec unregister_needs_messages(database_id()) :: :ok
  def unregister_needs_messages(database_id) do
    Registry.unregister(TestMessagesRegistry, database_id)
  end

  @doc """
  Checks if a sequence needs more test messages (has less than 10).
  """
  @spec needs_test_messages?(database_id()) :: boolean()
  def needs_test_messages?(database_id) do
    # First check if any process is registered as needing messages
    case Registry.lookup(TestMessagesRegistry, database_id) do
      [] -> false
      _ -> true
    end
  end

  @doc """
  Adds a test message to the sequence's list if there are less than 10 messages.
  Returns true if the message was added, false otherwise.
  """
  @spec add_test_message(database_id(), table_oid(), consumer_message()) :: boolean()
  def add_test_message(database_id, table_oid, message) do
    Logger.info("[TestMessages] Adding test message to #{database_id} #{table_oid}")

    case :ets.lookup(:test_messages, {database_id, table_oid}) do
      [{{^database_id, ^table_oid}, messages}] ->
        if length(messages) < max_message_count() do
          :ets.insert(:test_messages, {{database_id, table_oid}, [message | messages]})
          true
        else
          false
        end

      [] ->
        :ets.insert(:test_messages, {{database_id, table_oid}, [message]})
        true
    end
  end

  @spec delete_test_message(database_id(), table_oid(), String.t()) :: true | false
  def delete_test_message(database_id, table_oid, replication_message_trace_id) do
    case :ets.lookup(:test_messages, {database_id, table_oid}) do
      [{{^database_id, ^table_oid}, messages}] ->
        message_to_delete =
          Enum.find(messages, fn message -> message.replication_message_trace_id == replication_message_trace_id end)

        if message_to_delete do
          :ets.insert(
            :test_messages,
            {{database_id, table_oid},
             Enum.reject(messages, fn message -> message.replication_message_trace_id == replication_message_trace_id end)}
          )

          true
        else
          false
        end

      [] ->
        false
    end
  end

  @doc """
  Returns the list of test messages for a sequence.
  Returns an empty list if no messages exist for the sequence.
  """
  @spec get_test_messages(database_id(), table_oid()) :: [consumer_message()]
  def get_test_messages(database_id, table_oid) do
    case :ets.lookup(:test_messages, {database_id, table_oid}) do
      [{{^database_id, ^table_oid}, messages}] -> Enum.reverse(messages)
      [] -> []
    end
  end

  @doc """
  Deletes all test messages for a sequence.
  """
  @spec delete_test_messages(database_id(), table_oid()) :: true
  def delete_test_messages(database_id, table_oid) do
    :ets.delete(:test_messages, {database_id, table_oid})
  end

  @doc """
  Deletes the entire ETS table.
  """
  @spec destroy() :: true
  def destroy do
    :ets.delete(:test_messages)
  end
end
