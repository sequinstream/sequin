defmodule Sequin.Ecto.Error do
  @moduledoc false
  use Ecto.Type

  import Sequin.Error.Guards, only: [is_error: 1]

  alias Sequin.Error
  alias Sequin.JSON

  @type t :: Error.t()

  def type, do: :error

  def cast(data) when is_error(data) do
    {:ok, data}
  end

  def cast(_), do: :error

  def load(data) when is_map(data) do
    {:ok, JSON.decode_struct_with_type(data)}
  end

  def load(_), do: :error

  def dump(data) when is_map(data) do
    {:ok, JSON.encode_struct_with_type(data)}
  end

  def dump(_), do: :error
end
