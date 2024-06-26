defmodule Sequin.Factory.Support do
  @moduledoc """
  Defines support functions for internal use by factories.

  These are not intended to be exported for use by tests.
  """
  alias Ecto.Changeset

  @type attrs :: keyword() | map()
  @type id :: String.t()

  @spec apply_changeset(Changeset.t(), id() | nil) :: struct()
  def apply_changeset(%Changeset{} = changeset, id) do
    changeset
    |> Changeset.apply_action!(:insert)
    |> Map.put(:id, id || UUID.uuid4())
  end

  @spec apply_changeset(Changeset.t()) :: struct()
  def apply_changeset(%Changeset{} = changeset) do
    Changeset.apply_action!(changeset, :insert)
  end

  @spec merge_attributes(map() | struct(), attrs()) :: map() | struct()
  def merge_attributes(record, attrs) when is_list(attrs), do: merge_attributes(record, Map.new(attrs))

  def merge_attributes(%{__struct__: _} = record, attrs), do: struct!(record, attrs)

  def merge_attributes(record, attrs), do: Map.merge(record, attrs)
end
