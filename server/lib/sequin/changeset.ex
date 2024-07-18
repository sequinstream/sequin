defmodule Sequin.Changeset do
  @moduledoc false
  def validate_name(%Ecto.Changeset{valid?: false} = changeset), do: changeset

  def validate_name(%Ecto.Changeset{valid?: true} = changeset) do
    name = Ecto.Changeset.get_field(changeset, :name)

    if String.match?(name, ~r/^[a-zA-Z0-9_]+$/) do
      changeset
    else
      Ecto.Changeset.add_error(changeset, :name, "must contain only alphanumeric characters or underscores")
    end
  end
end
