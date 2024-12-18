defmodule Sequin.Extensions.MessageHandlerBehaviour do
  @moduledoc """
  Defines a behaviour for handling replication messages from Postgres.
  """

  alias Sequin.Replication.Message

  @doc """
  Callback invoked to handle a batch of replication messages.

  ## Parameters

    * `context` - Any context passed by the caller to Replication.
    * `messages` - A list of Record types (InsertedRecord, UpdatedRecord, or DeletedRecord) that Replication handles.

  ## Returns

    The return value is implementation-specific and may vary based on the needs of the consumer.
  """
  @callback handle_messages(context :: any(), messages :: [Message.t()]) :: any()

  @callback produce_messages(context :: any(), consumer_id :: String.t(), count :: non_neg_integer()) ::
              {context :: any(), messages :: [Message.t()]}
end
