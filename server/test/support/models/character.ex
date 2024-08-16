defmodule Sequin.Test.Support.Models.Character do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Query

  schema "characters" do
    field :name, :string
    field :house, :string
    field :planet, :string
    field :is_active, :boolean
    field :tags, {:array, :string}
  end

  def where_id(query \\ base_query(), id) do
    from(c in query, where: c.id == ^id)
  end

  defp base_query(query \\ __MODULE__) do
    from(c in query, as: :character)
  end
end
