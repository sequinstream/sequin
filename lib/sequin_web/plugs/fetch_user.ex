defmodule SequinWeb.Plugs.FetchUser do
  @moduledoc false
  import Plug.Conn

  alias Sequin.Accounts
  alias Sequin.Accounts.Account
  alias Sequin.Error
  alias SequinWeb.ApiFallbackPlug

  def init(opts), do: opts

  def call(conn, _opts) do
    case List.first(Accounts.list_accounts()) do
      %Account{} = account ->
        conn
        |> assign(:account, account)
        |> assign(:account_id, account.id)

      nil ->
        error =
          Error.unauthorized(
            message: """
            No account found.
            """
          )

        ApiFallbackPlug.call(conn, {:error, error})
    end
  end
end
