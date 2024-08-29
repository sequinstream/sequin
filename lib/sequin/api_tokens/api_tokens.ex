defmodule Sequin.ApiTokens do
  @moduledoc false
  import Ecto.Query

  alias Sequin.ApiTokens.ApiToken
  alias Sequin.Error
  alias Sequin.Repo

  @doc """
  Creates a token for the given org.
  """
  def create_for_account!(account_id, attrs) do
    account_id
    |> ApiToken.build_token()
    |> ApiToken.create_changeset(attrs)
    |> Repo.insert!()
  end

  def create_for_account(account_id, attrs) do
    account_id
    |> ApiToken.build_token()
    |> ApiToken.create_changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Finds a token by the given (unhashed) token.
  """
  def find_by_token(token) do
    token
    |> ApiToken.where_token()
    |> Repo.one()
    |> case do
      nil -> {:error, Error.not_found(entity: :api_token)}
      token -> {:ok, token}
    end
  end

  def list_tokens_for_account(account_id) do
    Repo.all(from t in ApiToken, where: t.account_id == ^account_id)
  end

  def get_token_by(params) do
    Repo.get_by(ApiToken, params)
  end

  def delete_token_for_account(token_id, account_id) do
    case get_token_by(id: token_id, account_id: account_id) do
      nil ->
        {:error, Error.not_found(entity: :api_token)}

      token ->
        Repo.delete(token)
    end
  end
end
