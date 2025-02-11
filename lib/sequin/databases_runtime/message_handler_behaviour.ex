defmodule Sequin.DatabasesRuntime.SlotProcessor.MessageHandlerBehaviour do
  @moduledoc """
  Defines a behaviour for handling replication messages from Postgres.
  """

  alias Sequin.DatabasesRuntime.PostgresAdapter.Decoder.Messages.LogicalMessage
  alias Sequin.DatabasesRuntime.SlotProcessor.Message

  @doc """
  Callback invoked to handle a batch of replication messages.

  ## Parameters

    * `context` - Any context passed by the caller to SlotProcessor.
    * `messages` - A list of Record types (InsertedRecord, UpdatedRecord, or DeletedRecord) that SlotProcessor handles.
    * `high_watermark_wal_cursor` - The highest WAL cursor that is flushed to MessageHandler.

  ## Returns

    The return value is an updated context.
  """
  @callback handle_messages(
              context :: any(),
              messages :: [Message.t()],
              high_watermark_wal_cursor :: Replication.wal_cursor()
            ) ::
              {:ok, count :: non_neg_integer(), context :: any()} | {:error, reason :: any()}

  @doc """
  Callback invoked to handle a single logical message.

  ## Parameters

    * `context` - Any context passed by the caller to SlotProcessor.
    * `message` - A single logical message.

  ## Returns

    The return value is an updated context.
  """
  @callback handle_logical_message(context :: any(), seq :: non_neg_integer(), message :: LogicalMessage.t()) ::
              {:ok, context :: any()} | {:error, reason :: any()}
end
