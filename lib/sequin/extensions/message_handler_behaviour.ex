defmodule Sequin.Extensions.MessageHandlerBehaviour do
  @moduledoc """
  Defines a behaviour for handling replication messages from Postgres.
  """

  alias Sequin.Extensions.PostgresAdapter.Decoder.Messages.LogicalMessage
  alias Sequin.Replication.Message

  @doc """
  Callback invoked to handle a batch of replication messages.

  ## Parameters

    * `context` - Any context passed by the caller to Replication.
    * `messages` - A list of Record types (InsertedRecord, UpdatedRecord, or DeletedRecord) that Replication handles.

  ## Returns

    The return value is an updated context.
  """
  @callback handle_messages(context :: any(), messages :: [Message.t()]) ::
              {:ok, count :: non_neg_integer(), context :: any()} | {:error, reason :: any()}

  @doc """
  Callback invoked to handle a single logical message.

  ## Parameters

    * `context` - Any context passed by the caller to Replication.
    * `message` - A single logical message.

  ## Returns

    The return value is an updated context.
  """
  @callback handle_logical_message(context :: any(), message :: LogicalMessage.t()) ::
              {:ok, context :: any()} | {:error, reason :: any()}
end
