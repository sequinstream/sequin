defmodule Sequin.Transforms.TestMessages do
  @moduledoc """
  Manages :ets table for storing messages for testing transforms.

  Users use these messages to see the before/after when writing ie. PathTransforms in the console.
  """

  @type sequence_id :: String.t()
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
  Checks if a sequence needs more test messages (has less than 10).
  """
  @spec needs_test_messages?(sequence_id()) :: boolean()
  def needs_test_messages?(sequence_id) do
    case :ets.lookup(:test_messages, sequence_id) do
      [{^sequence_id, messages}] -> length(messages) < 10
      [] -> true
    end
  end

  @doc """
  Adds a test message to the sequence's list if there are less than 10 messages.
  Returns true if the message was added, false otherwise.
  """
  @spec add_test_message(sequence_id(), consumer_message()) :: boolean()
  def add_test_message(sequence_id, message) do
    case :ets.lookup(:test_messages, sequence_id) do
      [{^sequence_id, messages}] ->
        if length(messages) < 10 do
          :ets.insert(:test_messages, {sequence_id, [message | messages]})
          true
        else
          false
        end

      [] ->
        :ets.insert(:test_messages, {sequence_id, [message]})
        true
    end
  end

  @doc """
  Returns the list of test messages for a sequence.
  Returns an empty list if no messages exist for the sequence.
  """
  @spec get_test_messages(sequence_id()) :: [consumer_message()]
  def get_test_messages(sequence_id) do
    case :ets.lookup(:test_messages, sequence_id) do
      [{^sequence_id, messages}] -> messages
      [] -> []
    end
  end

  @doc """
  Deletes all test messages for a sequence.
  """
  @spec delete_sequence(sequence_id()) :: true
  def delete_sequence(sequence_id) do
    :ets.delete(:test_messages, sequence_id)
  end

  @doc """
  Deletes the entire ETS table.
  """
  @spec destroy() :: true
  def destroy do
    :ets.delete(:test_messages)
  end
end
