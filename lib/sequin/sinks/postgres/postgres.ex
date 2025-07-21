defmodule Sequin.Sinks.Postgres do
  @moduledoc false
  alias Sequin.Consumers.PostgresSink
  alias Sequin.Error
  alias Sequin.Sinks.Postgres.Implementation

  @module Application.compile_env(:sequin, :postgres_module, Implementation)

  @callback insert_records(PostgresSink.t(), [map()]) :: :ok | {:error, Error.t()}
  @callback upsert_records(PostgresSink.t(), [map()]) :: :ok | {:error, Error.t()}
  @callback test_connection(PostgresSink.t()) :: :ok | {:error, Error.t()}

  @spec insert_records(PostgresSink.t(), [map()]) :: :ok | {:error, Error.t()}
  def insert_records(%PostgresSink{} = sink, records) do
    @module.insert_records(sink, records)
  end

  @spec upsert_records(PostgresSink.t(), [map()]) :: :ok | {:error, Error.t()}
  def upsert_records(%PostgresSink{} = sink, records) do
    @module.upsert_records(sink, records)
  end

  @spec test_connection(PostgresSink.t()) :: :ok | {:error, Error.t()}
  def test_connection(%PostgresSink{} = sink) do
    @module.test_connection(sink)
  end
end
