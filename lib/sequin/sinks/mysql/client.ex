defmodule Sequin.Sinks.Mysql.Client do
  @moduledoc """
  Client for interacting with MySQL databases using MyXQL.
  """

  alias Decimal
  alias Sequin.Consumers.MysqlSink
  alias Sequin.Error
  alias Sequin.Sinks.Mysql.ConnectionCache

  require Logger

  @doc """
  Test the connection to the MySQL database.
  """
  def test_connection(%MysqlSink{} = sink) do
    case ConnectionCache.test_connection(sink) do
      :ok -> :ok
      {:error, error} -> {:error, format_error(error)}
    end
  end

  @doc """
  Insert or update records in batch.
  """
  def upsert_records(%MysqlSink{} = sink, records) when is_list(records) do
    if Enum.empty?(records) do
      {:ok}
    else
      case ConnectionCache.get_connection(sink) do
        {:ok, pid} ->
          try do
            if sink.upsert_on_duplicate do
              insert_or_update_records(pid, sink, records)
            else
              insert_records(pid, sink, records)
            end
          rescue
            error ->
              {:error, format_error(error)}
          end

        {:error, error} ->
          {:error, format_error(error)}
      end
    end
  end

  @doc """
  Delete records by their primary keys.
  """
  def delete_records(%MysqlSink{} = sink, record_pks) when is_list(record_pks) do
    if Enum.empty?(record_pks) do
      {:ok}
    else
      case ConnectionCache.get_connection(sink) do
                  {:ok, pid} ->
            try do
              # Assume primary key is 'id' for simplicity
              placeholders = Enum.map(record_pks, fn _ -> "?" end) |> Enum.join(", ")
            delete_sql = "DELETE FROM `#{sink.table_name}` WHERE `id` IN (#{placeholders})"

            case MyXQL.query(pid, delete_sql, record_pks) do
              {:ok, %MyXQL.Result{num_rows: num_rows}} ->
                Logger.debug("[MySQL] Deleted #{num_rows} records from #{sink.table_name}")
                {:ok}

              {:error, error} ->
                {:error, format_error(error)}
            end
          rescue
            error ->
              {:error, format_error(error)}
          end

        {:error, error} ->
          {:error, format_error(error)}
      end
    end
  end

  defp insert_or_update_records(pid, %MysqlSink{} = sink, records) do
    case build_upsert_query(sink, records) do
      {:ok, {sql, params}} ->
        case MyXQL.query(pid, sql, params) do
          {:ok, %MyXQL.Result{num_rows: num_rows}} ->
            Logger.debug("[MySQL] Upserted #{num_rows} records to #{sink.table_name}")
            {:ok}

          {:error, error} ->
            {:error, format_error(error)}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp insert_records(pid, %MysqlSink{} = sink, records) do
    case build_insert_query(sink, records) do
      {:ok, {sql, params}} ->
        case MyXQL.query(pid, sql, params) do
          {:ok, %MyXQL.Result{num_rows: num_rows}} ->
            Logger.debug("[MySQL] Inserted #{num_rows} records to #{sink.table_name}")
            {:ok}

          {:error, error} ->
            {:error, format_error(error)}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp build_upsert_query(%MysqlSink{} = sink, records) do
    case extract_columns_and_values(records) do
      {:ok, {columns, values}} ->
        column_list = Enum.map(columns, &"`#{&1}`") |> Enum.join(", ")
        placeholders = Enum.map(columns, fn _ -> "?" end) |> Enum.join(", ")

        update_clause =
          columns
          |> Enum.map(&"`#{&1}` = VALUES(`#{&1}`)")
          |> Enum.join(", ")

        sql = """
        INSERT INTO `#{sink.table_name}` (#{column_list})
        VALUES #{Enum.map(values, fn _ -> "(#{placeholders})" end) |> Enum.join(", ")}
        ON DUPLICATE KEY UPDATE #{update_clause}
        """

        params = Enum.flat_map(values, & &1)

        {:ok, {sql, params}}

      {:error, error} ->
        {:error, error}
    end
  end

  defp build_insert_query(%MysqlSink{} = sink, records) do
    case extract_columns_and_values(records) do
      {:ok, {columns, values}} ->
        column_list = Enum.map(columns, &"`#{&1}`") |> Enum.join(", ")
        placeholders = Enum.map(columns, fn _ -> "?" end) |> Enum.join(", ")

        sql = """
        INSERT INTO `#{sink.table_name}` (#{column_list})
        VALUES #{Enum.map(values, fn _ -> "(#{placeholders})" end) |> Enum.join(", ")}
        """

        params = Enum.flat_map(values, & &1)

        {:ok, {sql, params}}

      {:error, error} ->
        {:error, error}
    end
  end

  defp extract_columns_and_values(records) do
    if Enum.empty?(records) do
      {:error, Error.service(service: :mysql, message: "No records provided")}
    else
      all_columns =
        records
        |> Enum.flat_map(&Map.keys/1)
        |> Enum.uniq()
        |> Enum.sort()

      values =
        Enum.map(records, fn record ->
          Enum.map(all_columns, fn column ->
            case Map.get(record, column) do
              nil -> nil
              value when is_binary(value) -> value
              value when is_number(value) -> value
              value when is_boolean(value) -> value
              %DateTime{} = dt -> dt
              %Date{} = date -> date
              %Time{} = time -> time
              %NaiveDateTime{} = naive_dt -> naive_dt
              %Decimal{} = decimal -> decimal
              value -> Jason.encode!(value)
            end
          end)
        end)

      {:ok, {all_columns, values}}
    end
  end

  defp format_error(%MyXQL.Error{} = error) do
    Error.service(
      service: :mysql,
      message: Exception.message(error),
      details: %{mysql_error: error}
    )
  end

  defp format_error(error) when is_binary(error) do
    Error.service(service: :mysql, message: error)
  end

  defp format_error(error) do
    Error.service(
      service: :mysql,
      message: "MySQL error: #{inspect(error)}",
      details: %{original_error: error}
    )
  end
end
