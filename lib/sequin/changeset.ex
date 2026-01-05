defmodule Sequin.Changeset do
  @moduledoc false
  import Ecto.Changeset

  alias Sequin.Replication.WalPipeline
  alias Sequin.WalPipeline.SourceTable

  require Logger

  def validate_name(%Ecto.Changeset{} = changeset) do
    name = Ecto.Changeset.get_field(changeset, :name)

    cond do
      is_nil(name) ->
        changeset

      String.match?(name, ~r/^[a-zA-Z0-9_\-.]+$/) ->
        changeset

      true ->
        Ecto.Changeset.add_error(
          changeset,
          :name,
          "Can only contain alphanumeric characters, underscores, hyphens, or dots"
        )
    end
  end

  def cast_embed(%Ecto.Changeset{valid?: false} = changeset, :source_tables), do: changeset

  def cast_embed(%Ecto.Changeset{data: %WalPipeline{}} = changeset, :source_tables) do
    cast_embed(changeset, :source_tables, with: &SourceTable.event_changeset(&1, &2))
  end

  def cast_embed(%Ecto.Changeset{} = changeset, :source_tables) do
    cast_embed(changeset, :source_tables, with: &SourceTable.event_changeset(&1, &2))
  end

  # See: https://github.com/sequinstream/sequin/issues/1465
  def put_deserializers(%Ecto.Changeset{} = changeset, field, deserializer_field) do
    case get_field(changeset, field) do
      nil ->
        changeset

      field_value ->
        deserializers =
          field_value
          |> Enum.map(fn
            {key, %Date{}} -> {key, "date"}
            {key, %DateTime{}} -> {key, "datetime"}
            {key, %NaiveDateTime{}} -> {key, "naive_datetime"}
            {key, %Decimal{}} -> {key, "decimal"}
            {key, _} -> {key, nil}
          end)
          |> Enum.filter(fn {_key, type} -> type != nil end)
          |> Map.new()

        put_change(changeset, deserializer_field, deserializers)
    end
  end

  def deserialize(map, nil), do: map

  def deserialize(map, deserializers) when is_map(map) and is_map(deserializers) do
    Enum.reduce(map, map, fn {key, value}, acc ->
      case Map.get(deserializers, key) do
        nil ->
          acc

        "date" ->
          Map.replace!(acc, key, Date.from_iso8601!(value))

        "datetime" ->
          {:ok, datetime, _} = DateTime.from_iso8601(value)
          Map.replace!(acc, key, datetime)

        "naive_datetime" ->
          Map.replace!(acc, key, NaiveDateTime.from_iso8601!(value))

        "decimal" ->
          Map.replace!(acc, key, Decimal.new(value))

        unknown ->
          Logger.warning("Unknown deserializer: #{inspect(unknown)}")
          acc
      end
    end)
  end

  def annotations_check_constraint(%Ecto.Changeset{} = changeset) do
    check_constraint(changeset, :annotations, name: :annotations_size_limit, message: "annotations size limit exceeded")
  end
end
