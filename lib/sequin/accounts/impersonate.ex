defmodule Sequin.Accounts.Impersonate do
  @moduledoc """
  Helps impersonate users in the console/api.

  # Console

  1. `make impersonate org="org_id or org_name or org_slug"`
  2. Click the generated link
  """

  alias Sequin.Accounts
  alias Sequin.Error

  require Logger

  def getdel_secret(secret) do
    case Redix.command(:redix, ["GETDEL", secret]) do
      {:ok, nil} ->
        {:error, Error.not_found(entity: :secret)}

      {:ok, value} ->
        [admin_user_id, account_id] = String.split(value, ":")
        {:ok, %{admin_user_id: admin_user_id, account_id: account_id}}
    end
  end

  def generate_link(admin_user_id, account_id) when is_binary(admin_user_id) and is_binary(account_id) do
    case find_account(account_id) do
      {:ok, account} ->
        generate_link(admin_user_id, account)

      {:error, error} ->
        print("Error: #{Exception.message(error)}")

        {:error, error}
    end
  end

  def generate_link(admin_user_id, %Accounts.Account{} = account) when is_binary(admin_user_id) do
    secret = 48 |> :crypto.strong_rand_bytes() |> Base.encode32(padding: false)

    {:ok, _} = Redix.command(:redix, ["SET", secret, "#{admin_user_id}:#{account.id}", "PX", :timer.minutes(1)])

    case Application.fetch_env!(:sequin, :env) do
      :dev ->
        port = Application.fetch_env!(:sequin, SequinWeb.Endpoint)[:http][:port]
        print(~s(http://localhost:#{port}/admin/impersonate/#{secret}))

      _ ->
        host = Application.fetch_env!(:sequin, SequinWeb.Endpoint)[:url][:host]
        print(~s(https://#{host}/admin/impersonate/#{secret}))
    end

    {:ok, secret}
  end

  defp find_account(account_id) do
    Accounts.get_account(account_id)
  end

  if Mix.env() == :test do
    defp print(msg), do: Logger.info(msg)
  else
    # credo:disable-for-next-line
    defp print(msg), do: IO.puts(msg)
  end
end
