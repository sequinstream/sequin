defmodule Sequin.DatabasesRuntime.RecordHandlerBehaviour do
  @moduledoc """
  Defines a behaviour for handling records from Postgres' TableProducer.
  """

  @doc """
  Callback to initialize context that will be passed to `handle_records/2`.
  """
  @callback init(context :: any()) :: any()

  @doc """
  Callback invoked to handle a batch of records.

  ## Parameters

    * `context` - Any context passed by the caller to TableProducer.
    * `records` - A list of plain maps, representing records from Postgres.

  ## Returns

    The return value is implementation-specific and may vary based on the needs of the consumer.
  """
  @callback handle_records(context :: any(), records :: [map()]) :: {:ok, non_neg_integer()}
end
