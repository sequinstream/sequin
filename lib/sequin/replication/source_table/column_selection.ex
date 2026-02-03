defmodule Sequin.WalPipeline.SourceTable.ColumnSelection do
  @moduledoc false

  alias Sequin.Runtime.SlotProcessor.Message.Field
  alias Sequin.WalPipeline.SourceTable

  @doc """
  Filters a list of fields based on the source table's column selection configuration.

  Returns all fields if no column selection is configured.
  """
  @spec filter_fields([Field.t()], SourceTable.t()) :: [Field.t()]
  def filter_fields(fields, %SourceTable{} = source_table) do
    cond do
      # Include specific columns
      is_list(source_table.include_column_attnums) and source_table.include_column_attnums != [] ->
        Enum.filter(fields, fn field ->
          field.column_attnum in source_table.include_column_attnums
        end)

      # Exclude specific columns
      is_list(source_table.exclude_column_attnums) and source_table.exclude_column_attnums != [] ->
        Enum.reject(fields, fn field ->
          field.column_attnum in source_table.exclude_column_attnums
        end)

      # No filtering configured
      true ->
        fields
    end
  end

  @doc """
  Filters column attnums from a list of columns (used during backfills).
  """
  @spec filter_column_attnums([integer()], SourceTable.t() | nil) :: [integer()]
  def filter_column_attnums(column_attnums, nil), do: column_attnums

  def filter_column_attnums(column_attnums, %SourceTable{} = source_table) do
    cond do
      # Include specific columns
      is_list(source_table.include_column_attnums) and source_table.include_column_attnums != [] ->
        Enum.filter(column_attnums, fn attnum ->
          attnum in source_table.include_column_attnums
        end)

      # Exclude specific columns
      is_list(source_table.exclude_column_attnums) and source_table.exclude_column_attnums != [] ->
        Enum.reject(column_attnums, fn attnum ->
          attnum in source_table.exclude_column_attnums
        end)

      # No filtering configured
      true ->
        column_attnums
    end
  end

  @doc """
  Returns true if column selection is configured (either include or exclude).
  """
  @spec has_column_selection?(SourceTable.t()) :: boolean()
  def has_column_selection?(%SourceTable{} = source_table) do
    (is_list(source_table.include_column_attnums) and source_table.include_column_attnums != []) or
      (is_list(source_table.exclude_column_attnums) and source_table.exclude_column_attnums != [])
  end
end
