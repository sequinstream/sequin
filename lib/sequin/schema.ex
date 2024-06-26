defmodule Sequin.Schema do
  @moduledoc false
  @type id :: String.t()
  @type t :: Ecto.Schema.t()

  defmacro __using__(_) do
    quote do
      use Ecto.Schema
      use TypedEctoSchema

      @primary_key {:id, :binary_id, read_after_writes: true}
      @foreign_key_type :binary_id
      @timestamps_opts [type: :utc_datetime]
    end
  end
end
