defmodule Sequin.Consumers.S2Sink do
  @moduledoc false

  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Encrypted.Field, as: EncryptedField

  @derive {Jason.Encoder, only: [:token, :stream, :basin]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:s2], default: :s2
    field :token, EncryptedField
    field :stream, :string
    field :basin, :string
    field :connection_id, :string
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [
      :token,
      :stream,
      :basin
    ])
    |> validate_required([:token, :stream, :basin])
    |> put_connection_id()
  end

  defp put_connection_id(changeset) do
    case get_field(changeset, :connection_id) do
      nil -> put_change(changeset, :connection_id, Ecto.UUID.generate())
      _ -> changeset
    end
  end

  def connection_opts(%__MODULE__{} = sink) do
    %{
      token: sink.token,
      stream: sink.stream,
      basin: sink.basin
    }
  end
end
