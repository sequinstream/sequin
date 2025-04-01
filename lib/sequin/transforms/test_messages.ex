defmodule Sequin.Transforms.TestMessages do
  @moduledoc """
  Manages :ets table for storing messages for testing transforms.

  Users use these messages to see the before/after when writing ie. PathTransforms in the console.
  """

  alias Sequin.Transforms.TestMessagesRegistry

  def registry, do: TestMessagesRegistry

  @type account_id :: String.t()
  @type consumer_message :: Sequin.Consumers.ConsumerRecord.t() | Sequin.Consumers.ConsumerEvent.t()
  @type t :: :ets.tid()

  @doc """
  Creates a new ETS table for storing test messages.
  """
  @spec create_ets_table() :: t()
  def create_ets_table do
    :ets.new(:test_messages, [:named_table, :set, :public])
  rescue
    ArgumentError ->
      :ok
  end

  @doc """
  Registers that a process needs test messages for a sequence.
  """
  @spec register_needs_messages(account_id()) :: {:ok, any()} | {:error, any()}
  def register_needs_messages(account_id) do
    Registry.register(TestMessagesRegistry, account_id, :ok)
  end

  @doc """
  Checks if a sequence needs more test messages (has less than 10).
  """
  @spec needs_test_messages?(account_id()) :: boolean()
  def needs_test_messages?(account_id) do
    # First check if any process is registered as needing messages
    case Registry.lookup(TestMessagesRegistry, account_id) do
      [] ->
        false

      _ ->
        # Then check if we have less than 10 messages
        case :ets.lookup(:test_messages, account_id) do
          [{^account_id, messages}] -> length(messages) < 10
          [] -> true
        end
    end
  end

  @doc """
  Adds a test message to the sequence's list if there are less than 10 messages.
  Returns true if the message was added, false otherwise.
  """
  @spec add_test_message(account_id(), consumer_message()) :: boolean()
  def add_test_message(account_id, message) do
    case :ets.lookup(:test_messages, account_id) do
      [{^account_id, messages}] ->
        if length(messages) < 10 do
          :ets.insert(:test_messages, {account_id, [message | messages]})
          true
        else
          false
        end

      [] ->
        :ets.insert(:test_messages, {account_id, [message]})
        true
    end
  end

  @doc """
  Returns the list of test messages for a sequence.
  Returns an empty list if no messages exist for the sequence.
  """
  @spec get_test_messages(account_id()) :: [consumer_message()]
  def get_test_messages(account_id) do
    case :ets.lookup(:test_messages, account_id) do
      [{^account_id, messages}] -> messages
      [] -> []
    end
  end

  @doc """
  Deletes all test messages for a sequence.
  """
  @spec delete_sequence(account_id()) :: true
  def delete_sequence(account_id) do
    :ets.delete(:test_messages, account_id)
  end

  @doc """
  Deletes the entire ETS table.
  """
  @spec destroy() :: true
  def destroy do
    :ets.delete(:test_messages)
  end
end
