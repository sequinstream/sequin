defmodule Sequin.Streams.Migrations do
  @moduledoc false
  alias Ecto.Adapters.Postgres.Connection
  alias Ecto.Migration.Table
  alias Sequin.Postgres
  alias Sequin.Repo
  alias Sequin.Streams.StreamTable
  alias Sequin.Streams.StreamTableColumn

  @spec provision_stream_table(%StreamTable{}) :: :ok | {:error, Postgrex.Error.t()}
  def provision_stream_table(%StreamTable{} = stream_table) do
    table_name = stream_table.table_name
    schema_name = stream_table.table_schema_name

    required_columns = [
      {:add, :sequin_id, :uuid, primary_key: stream_table.insert_mode == :append},
      {:add, :seq, :bigserial, null: false},
      {:add, :data, :text, []},
      {:add, :recorded_at, :timestamp, null: false},
      {:add, :deleted, :boolean, null: false}
    ]

    custom_columns = Enum.map(stream_table.columns, &stream_table_column_to_ecto_column/1)

    table = %Table{
      name: table_name,
      prefix: schema_name,
      primary_key: false
    }

    create_stmt = {:create, table, required_columns ++ custom_columns}

    ddl_sql = Connection.execute_ddl(create_stmt)

    with {:ok, _} <- Repo.query(ddl_sql) do
      :ok
    end
  end

  @spec migrate_stream_table(%StreamTable{}, %StreamTable{}) :: :ok | {:error, Postgrex.Error.t()}
  def migrate_stream_table(old_stream_table, new_stream_table) do
    # Migrate columns first, they will all refer to old table name and schema name
    column_ddl_sql = migrate_stream_table_columns_sql(old_stream_table, new_stream_table)
    table_ddl_sql = migrate_stream_table_table_sql(old_stream_table, new_stream_table)

    Enum.reduce_while(column_ddl_sql ++ table_ddl_sql, :ok, fn ddl, :ok ->
      case Repo.query(ddl) do
        {:ok, _} -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  @spec drop_stream_table(%StreamTable{}) :: :ok | {:error, Postgrex.Error.t()}
  def drop_stream_table(%StreamTable{} = stream_table) do
    table = %Table{
      name: stream_table.table_name,
      prefix: stream_table.table_schema_name
    }

    ddl_command = {:drop, table, :cascade}
    ddl_sql = Connection.execute_ddl(ddl_command)

    case Repo.query(ddl_sql) do
      {:ok, _result} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp stream_table_column_to_ecto_column(%StreamTableColumn{} = column) do
    {:add, column.name, ecto_type(column.type), column_options(column)}
  end

  defp ecto_type(:text), do: :string
  defp ecto_type(:integer), do: :integer
  defp ecto_type(:boolean), do: :boolean
  defp ecto_type(:timestamp), do: :utc_datetime

  defp column_options(column) do
    options = []
    options = if column.is_pk, do: [{:primary_key, true} | options], else: options
    options
  end

  defp migrate_stream_table_columns_sql(old_stream_table, new_stream_table) do
    changes = compare_stream_table_columns(old_stream_table, new_stream_table)
    # We work with the old table name and schema name here
    table = %Table{name: old_stream_table.table_name, prefix: old_stream_table.table_schema_name}

    ddl_commands =
      Enum.map(changes, fn change ->
        case change do
          {:add_column, column} ->
            {:alter, table, [{:add, column.name, ecto_type(column.type), column_options(column)}]}

          {:drop_column, column_name} ->
            {:alter, table, [{:remove, column_name}]}

          {:rename_column, old_name, new_name} ->
            {:rename, table, old_name, new_name}
        end
      end)

    Enum.map(ddl_commands, &Connection.execute_ddl/1)
  end

  defp migrate_stream_table_table_sql(old_stream_table, new_stream_table) do
    # Then, migrate the table name and schema name
    %{table_name: old_table_name, table_schema_name: old_table_schema_name} = old_stream_table
    %{table_name: new_table_name, table_schema_name: new_table_schema_name} = new_stream_table

    rename_table_ddl = rename_table_ddl(old_table_schema_name, old_table_name, new_table_name)
    change_schema_ddl = change_schema_ddl(old_table_schema_name, new_table_name, new_table_schema_name)

    ddl = if old_table_name == new_table_name, do: [], else: [rename_table_ddl]
    # Run the change schema after the table name change
    if old_table_schema_name == new_table_schema_name, do: ddl, else: ddl ++ [change_schema_ddl]
  end

  defp compare_stream_table_columns(old, new) do
    old_columns = Map.new(old.columns, &{&1.id, &1})
    new_columns = Map.new(new.columns, &{&1.id, &1})

    added = Enum.reject(new.columns, &Map.has_key?(old_columns, &1.id))
    dropped = Enum.reject(old.columns, &Map.has_key?(new_columns, &1.id))

    renamed =
      for {id, new_col} <- new_columns, old_col = old_columns[id], old_col.name != new_col.name do
        {:rename_column, old_col.name, new_col.name}
      end

    Enum.map(added, &{:add_column, &1}) ++
      Enum.map(dropped, &{:drop_column, &1.name}) ++
      renamed
  end

  defp rename_table_ddl(old_table_schema_name, old_table_name, new_table_name) do
    "alter table #{Postgres.quote_name(old_table_schema_name, old_table_name)} rename to #{Postgres.quote_name(new_table_name)}"
  end

  # There is no set schema command in the Postgres adapter
  defp change_schema_ddl(old_table_schema_name, table_name, new_table_schema_name) do
    "alter table #{Postgres.quote_name(old_table_schema_name, table_name)} set schema #{Postgres.quote_name(new_table_schema_name)}"
  end
end
