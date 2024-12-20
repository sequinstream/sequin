defmodule Sequin.Encrypted.Field do
  @moduledoc """
  Encrypt embedded schema fields.

  Used for encrypting nested fields, just specify the field type as Encrypted field.
  For example usage check: `Ghola.Sources.Airtable.ResourceDefinition` schema
  """
  use Cloak.Ecto.Binary, vault: Sequin.Vault

  @type t :: String.t()

  def embed_as(:json), do: :dump

  def dump(nil), do: {:ok, nil}

  def dump(value) do
    with {:ok, encrypted} <- super(value) do
      {:ok, Base.encode64(encrypted)}
    end
  end

  def equal?(v1, v2), do: v1 == v2

  def load(nil), do: super(nil)

  def load(value) do
    super(Base.decode64!(value))
  end
end
