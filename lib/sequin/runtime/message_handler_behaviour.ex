defmodule Sequin.Runtime.SlotProcessor.MessageHandlerBehaviour do
  @moduledoc """
  Defines a behaviour for handling replication messages from Postgres.
  """

  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.LogicalMessage
  alias Sequin.Runtime.SlotProcessor.Message
  alias Sequin.Runtime.SlotProcessor.MessageHandler.Context

  @doc """
  Callback invoked before handling a batch of replication messages.

  ## Parameters

    * `context` - Any context passed by the caller to SlotProcessor.
    * `messages` - A list of Record types (InsertedRecord, UpdatedRecord, or DeletedRecord) that SlotProcessor handles.

  ## Returns

    The return value is :ok or {:error, reason}.
  """
  @callback before_handle_messages(context :: Context.t(), messages :: [Message.t()]) ::
              :ok | {:error, reason :: any()}

  @doc """
  Callback invoked to handle a batch of replication messages.

  ## Parameters

    * `context` - Any context passed by the caller to SlotProcessor.
    * `messages` - A list of Record types (InsertedRecord, UpdatedRecord, or DeletedRecord) that SlotProcessor handles.

  ## Returns

    The return value is an updated context.
  """
  @callback handle_messages(context :: Context.t(), messages :: [Message.t()]) ::
              {:ok, count :: non_neg_integer()} | {:error, reason :: any()}

  @doc """
  Callback invoked to handle a single logical message.

  ## Parameters

    * `context` - Any context passed by the caller to SlotProcessor.
    * `message` - A single logical message.

  ## Returns

    The return value is an updated context.
  """
  @callback handle_logical_message(context :: Context.t(), seq :: non_neg_integer(), message :: LogicalMessage.t()) ::
              :ok | {:error, reason :: any()}
end
