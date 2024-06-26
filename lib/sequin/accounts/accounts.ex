defmodule Sequin.Accounts do
  @moduledoc false
  alias Sequin.Accounts.Account
  alias Sequin.Repo

  def list, do: Repo.all(Account)

  def create(attrs) do
    %Account{}
    |> Account.changeset(attrs)
    |> Repo.insert()
  end
end
