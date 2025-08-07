defmodule Sequin.Sinks.Mysql.Client do
  @moduledoc """
  Client for interacting with MySQL databases using MyXQL.
  """

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
            placeholders = Enum.map_join(record_pks, ", ", fn _ -> "?" end)
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
        column_list = Enum.map_join(columns, ", ", &"`#{&1}`")
        placeholders = Enum.map_join(columns, ", ", fn _ -> "?" end)

        update_clause = Enum.map_join(columns, ", ", &"`#{&1}` = VALUES(`#{&1}`)")

        sql = """
        INSERT INTO `#{sink.table_name}` (#{column_list})
        VALUES #{Enum.map_join(values, ", ", fn _ -> "(#{placeholders})" end)}
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
        column_list = Enum.map_join(columns, ", ", &"`#{&1}`")
        placeholders = Enum.map_join(columns, ", ", fn _ -> "?" end)

        sql = """
        INSERT INTO `#{sink.table_name}` (#{column_list})
        VALUES #{Enum.map_join(values, ", ", fn _ -> "(#{placeholders})" end)}
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

  defp format_error(%DBConnection.ConnectionError{reason: reason, message: message} = error) do
    message = format_connection_error_reason(reason, message)

    Error.service(
      service: :mysql,
      message: message,
      details: %{original_error: error}
    )
  end

  defp format_error(%MyXQL.Error{mysql: %{code: code, name: name}} = error) when not is_nil(code) do
    message = format_mysql_error(code, name, Exception.message(error))

    Error.service(
      service: :mysql,
      message: message,
      details: %{mysql_error: error, code: code, name: name}
    )
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

  defp format_connection_error_reason(reason, message) do
    # First check the reason field
    case reason do
      :queue_timeout ->
        """
        Connection test failed. Unable to connect to MySQL database.

        Please check:
        • Database server is running and accessible
        • Host and port are correct
        • Network connectivity (firewall, security groups)
        • Username and password are valid
        • Database exists and user has access permissions
        """

      :error ->
        # For :error reason, parse the message for more specific error types
        parse_connection_message(message)

      _ ->
        "Connection failed: #{inspect(reason)}"
    end
  end

  defp parse_connection_message(message) when is_binary(message) do
    cond do
      String.contains?(message, "econnrefused") ->
        """
        Connection refused. Please check:
        • The database server is running
        • The host and port are correct
        • Firewall settings allow connections
        """

      String.contains?(message, "timeout") ->
        """
        Connection timed out. Please verify:
        • The hostname and port are correct
        • The database server is running
        • Network connectivity is available
        """

      String.contains?(message, "nxdomain") ->
        "Unable to resolve the hostname. Please check if the hostname is correct."

      String.contains?(message, "Access denied") ->
        "Authentication failed. Please verify your username and password."

      true ->
        """
        Connection failed. Please verify:
        • Database server is running and accessible
        • Host and port are correct
        • Username and password are valid
        • Database name is correct

        Technical details: #{message}
        """
    end
  end

  defp parse_connection_message(message) do
    "Connection failed: #{inspect(message)}"
  end

  defp format_mysql_error(code, name, original_message) do
    case code do
      1045 -> "Authentication failed. Please verify your username and password."
      1049 -> "Database '#{extract_database_name(original_message)}' does not exist. Please verify the database name."
      1044 -> "Access denied to database. Please verify the user has access permissions."
      1146 -> "Table does not exist. Please verify the table name and ensure it exists in the database."
      1062 -> "Duplicate entry error. #{original_message}"
      1054 -> "Unknown column error. #{original_message}"
      _ -> "MySQL error (#{code}/#{name}): #{original_message}"
    end
  end

  defp extract_database_name(message) do
    case Regex.run(~r/database '([^']+)'/, message) do
      [_, db_name] -> db_name
      _ -> "unknown"
    end
  end
end
