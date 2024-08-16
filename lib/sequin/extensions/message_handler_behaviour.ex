defmodule Sequin.Extensions.ReplicationMessageHandlerBehaviour do
  @moduledoc """
  Defines a behaviour for handling replication messages from Postgres.
  """

  alias Sequin.Extensions.PostgresAdapter.Changes.DeletedRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.InsertedRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.UpdatedRecord

  @doc """
  Callback invoked to handle a batch of replication messages.

  ## Parameters

    * `context` - Any context passed by the caller to Replication.
    * `messages` - A list of Record types (InsertedRecord, UpdatedRecord, or DeletedRecord) that Replication handles.

  ## Returns

    The return value is implementation-specific and may vary based on the needs of the consumer.
  """
  @callback handle_messages(context :: any(), messages :: [InsertedRecord.t() | UpdatedRecord.t() | DeletedRecord.t()]) ::
              any()
end
