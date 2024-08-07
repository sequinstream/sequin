defmodule Sequin.Streams.Migrations do
  @moduledoc false
  alias Ecto.Adapters.Postgres.Connection
  alias Ecto.Migration.Index
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
      {:add, :sequin_id, :uuid, primary_key: true},
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

    create_ddl = execute_ddl({:create, table, required_columns ++ custom_columns})

    indexes = indexes(stream_table)
    conflict_key_index = conflict_key_index(stream_table) || nil

    index_ddls =
      Enum.map(indexes ++ List.wrap(conflict_key_index), fn %Index{} = index -> execute_ddl({:create, index}) end)

    execute_sql([create_ddl] ++ index_ddls)
  end

  @spec migrate_stream_table(%StreamTable{}, %StreamTable{}) :: :ok | {:error, Postgrex.Error.t()}
  def migrate_stream_table(old_stream_table, new_stream_table) do
    # Migrate columns first, they will all refer to old table name and schema name
    column_ddl_cmds = migrate_stream_table_columns_cmds(old_stream_table, new_stream_table)
    table_ddl_cmds = migrate_stream_table_cmds(old_stream_table, new_stream_table)

    (column_ddl_cmds ++ table_ddl_cmds)
    |> Enum.map(&execute_ddl/1)
    |> execute_sql()
  end

  @spec drop_stream_table(%StreamTable{}) :: :ok | {:error, Postgrex.Error.t()}
  def drop_stream_table(%StreamTable{} = stream_table) do
    table = %Table{
      name: stream_table.table_name,
      prefix: stream_table.table_schema_name
    }

    drop_ddl = execute_ddl({:drop, table, :cascade})

    execute_sql(drop_ddl)
  end

  defp stream_table_column_to_ecto_column(%StreamTableColumn{} = column) do
    {:add, column.name, ecto_type(column.type), []}
  end

  defp ecto_type(:text), do: :string
  defp ecto_type(:integer), do: :integer
  defp ecto_type(:boolean), do: :boolean
  defp ecto_type(:timestamp), do: :utc_datetime
  defp ecto_type(:uuid), do: :uuid

  defp migrate_stream_table_columns_cmds(old_stream_table, new_stream_table) do
    changes = compare_stream_table_columns(old_stream_table, new_stream_table)
    # We work with the old table name and schema name here
    table = %Table{name: old_stream_table.table_name, prefix: old_stream_table.table_schema_name}

    Enum.map(changes, fn change ->
      case change do
        {:add_column, column} ->
          {:alter, table, [stream_table_column_to_ecto_column(column)]}

        {:drop_column, column_name} ->
          {:alter, table, [{:remove, column_name}]}

        {:rename_column, old_name, new_name} ->
          {:rename, table, old_name, new_name}

        {:rename_index, _opts} = cmd ->
          cmd
      end
    end)
  end

  defp migrate_stream_table_cmds(old_stream_table, new_stream_table) do
    # Then, migrate the table name and schema name
    %{table_name: old_table_name, table_schema_name: old_table_schema_name} = old_stream_table
    %{table_name: new_table_name, table_schema_name: new_table_schema_name} = new_stream_table

    rename_table_cmd = {:rename_table, from: old_table_name, to: new_table_name, prefix: old_table_schema_name}

    change_schema_cmd =
      {:change_schema, from: old_table_schema_name, to: new_table_schema_name, table_name: new_table_name}

    ddl = if old_table_name == new_table_name, do: [], else: [rename_table_cmd]
    # Run the change schema after the table name change
    if old_table_schema_name == new_table_schema_name, do: ddl, else: ddl ++ [change_schema_cmd]
  end

  defp compare_stream_table_columns(%StreamTable{} = old, %StreamTable{} = new) do
    old_columns = Map.new(old.columns, &{&1.id, &1})
    new_columns = Map.new(new.columns, &{&1.id, &1})
    old_table_name = old.table_name
    new_table_name = new.table_name
    table_renamed? = old_table_name != new_table_name

    added = Enum.reject(new.columns, &Map.has_key?(old_columns, &1.id))
    dropped = Enum.reject(old.columns, &Map.has_key?(new_columns, &1.id))

    kept_columns =
      Enum.reduce(new.columns, [], fn new_col, acc ->
        if old_col = old_columns[new_col.id] do
          [{new_col, old_col} | acc]
        else
          acc
        end
      end)

    renamed_columns = Enum.filter(kept_columns, fn {new_col, old_col} -> old_col.name != new_col.name end)

    rename_column_cmds =
      for {new_col, old_col} <- renamed_columns do
        {:rename_column, old_col.name, new_col.name}
      end

    rename_index_cmds =
      if table_renamed? do
        for {new_col, old_col} <- kept_columns do
          old_index = column_index(old, old_col)
          new_index = column_index(new, new_col)
          # Note: we do not support changing the conflict keys on columns or the table insert_mode,
          # hence why we only care about old_index
          if old_index do
            {:rename_index, from: old_index.name, to: new_index.name, prefix: old.table_schema_name}
          end
        end
      else
        for {new_col, old_col} <- renamed_columns do
          old_index = column_index(new, old_col)
          new_index = column_index(new, new_col)

          if old_index do
            {:rename_index, from: old_index.name, to: new_index.name, prefix: old.table_schema_name}
          end
        end
      end

    rename_index_cmds = Enum.reject(rename_index_cmds, &is_nil/1)

    Enum.map(added, &{:add_column, &1}) ++
      Enum.map(dropped, &{:drop_column, &1.name}) ++
      rename_column_cmds ++
      rename_index_cmds
  end

  defp column_index(%StreamTable{} = stream_table, %StreamTableColumn{} = column) do
    %{table_name: table_name, table_schema_name: table_schema_name, insert_mode: insert_mode} = stream_table

    # We do not need to create an index in this situation, as we will use the conflict key as a unique constraint
    if insert_mode == :upsert and column.is_conflict_key and single_conflict_key?(stream_table) do
      nil
    else
      index_name = Postgres.identifier("idx", table_name, column.name)

      %Index{
        table: table_name,
        prefix: table_schema_name,
        name: index_name,
        columns: [column.name],
        unique: false,
        concurrently: concurrently()
      }
    end
  end

  defp single_conflict_key?(%StreamTable{} = stream_table) do
    Enum.count(stream_table.columns, & &1.is_conflict_key) == 1
  end

  defp indexes(%StreamTable{} = stream_table) do
    stream_table.columns
    |> Enum.map(&column_index(stream_table, &1))
    |> Enum.reject(&is_nil/1)
  end

  defp conflict_key_index(%StreamTable{} = stream_table) do
    %{table_name: table_name, table_schema_name: table_schema_name} = stream_table
    conflict_keys = Enum.filter(stream_table.columns, & &1.is_conflict_key)

    if Enum.empty?(conflict_keys) do
      nil
    else
      index_name = Postgres.identifier("idx", stream_table.table_name, "conflict_key")
      conflict_key_names = Enum.map(conflict_keys, & &1.name)

      %Index{table: table_name, prefix: table_schema_name, name: index_name, columns: conflict_key_names, unique: true}
    end
  end

  defp execute_sql(sql) do
    sql
    |> List.wrap()
    |> Enum.reduce_while(:ok, fn ddl, :ok ->
      case Repo.query(ddl) do
        {:ok, _} -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  # these commands are not supported by Ecto
  defp execute_ddl({:rename_index, opts}) do
    %{from: old_name, to: new_name, prefix: prefix} = Map.new(opts)
    "alter index #{Postgres.quote_name(prefix, old_name)} rename to #{Postgres.quote_name(new_name)}"
  end

  defp execute_ddl({:rename_table, opts}) do
    %{from: old_table_name, to: new_table_name, prefix: old_table_schema_name} = Map.new(opts)

    "alter table #{Postgres.quote_name(old_table_schema_name, old_table_name)} rename to #{Postgres.quote_name(new_table_name)}"
  end

  defp execute_ddl({:change_schema, opts}) do
    %{from: old_table_schema_name, to: new_table_schema_name, table_name: table_name} = Map.new(opts)

    "alter table #{Postgres.quote_name(old_table_schema_name, table_name)} set schema #{Postgres.quote_name(new_table_schema_name)}"
  end

  defp execute_ddl(tuple) do
    # Make sure it's not an iolist
    tuple |> Connection.execute_ddl() |> to_string()
  end

  defp concurrently do
    # Cannot run create index concurrently in test mode
    if env() == :test do
      false
    else
      true
    end
  end

  defp env do
    Application.get_env(:sequin, :env)
  end
end
