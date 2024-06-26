defmodule SequinStream.Accounts do
  alias SequinStream.Repo
  alias SequinStream.Accounts.Account

  def list, do: Repo.all(Account)

  def create(attrs) do
    %Account{}
    |> Account.changeset(attrs)
    |> Repo.insert()
  end
end
