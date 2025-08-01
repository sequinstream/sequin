defmodule Sequin.Sinks.Mysql do
  @moduledoc false
  alias Sequin.Consumers.MysqlSink
  alias Sequin.Error

  @callback test_connection(MysqlSink.t()) :: :ok | {:error, Error.t()}
  @callback upsert_records(MysqlSink.t(), [map()]) :: :ok | {:error, Error.t()}
  @callback delete_records(MysqlSink.t(), [any()]) :: :ok | {:error, Error.t()}

  @spec test_connection(MysqlSink.t()) :: :ok | {:error, Error.t()}
  def test_connection(%MysqlSink{} = sink) do
    impl().test_connection(sink)
  end

  @spec upsert_records(MysqlSink.t(), [map()]) :: :ok | {:error, Error.t()}
  def upsert_records(%MysqlSink{} = sink, records) when is_list(records) do
    impl().upsert_records(sink, records)
  end

  @spec delete_records(MysqlSink.t(), [any()]) :: :ok | {:error, Error.t()}
  def delete_records(%MysqlSink{} = sink, record_pks) when is_list(record_pks) do
    impl().delete_records(sink, record_pks)
  end

  defp impl do
    Application.get_env(:sequin, :mysql_module, Sequin.Sinks.Mysql.Client)
  end
end
