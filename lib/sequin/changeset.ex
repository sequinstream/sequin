defmodule Sequin.Changeset do
  @moduledoc false
  def validate_name(%Ecto.Changeset{} = changeset) do
    name = Ecto.Changeset.get_field(changeset, :name)

    cond do
      is_nil(name) ->
        changeset

      String.match?(name, ~r/^[a-zA-Z0-9_]+$/) ->
        changeset

      true ->
        Ecto.Changeset.add_error(changeset, :name, "must contain only alphanumeric characters or underscores")
    end
  end
end
