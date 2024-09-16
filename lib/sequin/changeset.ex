defmodule Sequin.Changeset do
  @moduledoc false
  import Ecto.Changeset

  alias Sequin.Consumers.SourceTable

  def validate_name(%Ecto.Changeset{} = changeset) do
    name = Ecto.Changeset.get_field(changeset, :name)

    cond do
      is_nil(name) ->
        changeset

      String.match?(name, ~r/^[a-zA-Z0-9_\-.]+$/) ->
        changeset

      true ->
        Ecto.Changeset.add_error(changeset, :name, "must contain only alphanumeric characters or underscores")
    end
  end

  def cast_embed(%Ecto.Changeset{valid?: false} = changeset, :source_tables), do: changeset

  def cast_embed(%Ecto.Changeset{} = changeset, :source_tables) do
    case get_field(changeset, :message_kind) do
      :record ->
        cast_embed(changeset, :source_tables, with: &SourceTable.record_changeset(&1, &2))

      :event ->
        cast_embed(changeset, :source_tables, with: &SourceTable.event_changeset(&1, &2))
    end
  end
end
