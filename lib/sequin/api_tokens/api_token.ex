defmodule Sequin.ApiTokens.ApiToken do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Accounts.Account
  alias Sequin.ApiTokens.ApiToken

  @rand_bytes 48
  @hash_algo :sha256

  @derive {Jason.Encoder, only: [:id, :name, :inserted_at]}
  schema "api_tokens" do
    belongs_to :account, Account
    field :name, :string
    field :hashed_token, :string
    field :token, Sequin.Encrypted.Binary

    timestamps(updated_at: false)
  end

  def create_changeset(struct, params \\ %{}) do
    struct
    |> cast(params, [:name, :token])
    |> validate_required([:name, :token])
    |> Sequin.Changeset.validate_name()
    |> unique_constraint([:name, :account_id], name: "api_tokens_account_id_name_index")
  end

  def build_token(account_id) do
    # Base64 to strip unwanted characters (makes copy/pasting easier in a command-line, as you can just
    # double-click the token string - + and / break that)
    token = @rand_bytes |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)
    hashed_token = :crypto.hash(@hash_algo, token)
    %ApiToken{account_id: account_id, token: token, hashed_token: hashed_token}
  end

  def where_token(query \\ base_query(), token) do
    hashed_token = :crypto.hash(@hash_algo, token)
    from([token: t] in query, where: t.hashed_token == ^hashed_token)
  end

  defp base_query(query \\ __MODULE__) do
    from(t in query, as: :token)
  end
end
