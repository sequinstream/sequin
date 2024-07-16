defmodule Sequin.Extensions.ReplicationMessageHandler do
  @moduledoc """
  Defines a behaviour for handling replication messages from Postgres.
  """

  alias Sequin.Extensions.PostgresAdapter.Changes.DeletedRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.NewRecord
  alias Sequin.Extensions.PostgresAdapter.Changes.UpdatedRecord

  @doc """
  Callback invoked to handle a replication message.

  ## Parameters

    * `context` - Any context passed by the caller to Replication.
    * `message` - One of the Record types (NewRecord, UpdatedRecord, or DeletedRecord) that Replication handles.

  ## Returns

    The return value is implementation-specific and may vary based on the needs of the consumer.
  """
  @callback handle_message(context :: any(), message :: NewRecord.t() | UpdatedRecord.t() | DeletedRecord.t()) :: any()
end
