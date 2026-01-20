defmodule Sequin.Accounts.Impersonate do
  @moduledoc """
  Helps impersonate users in the console/api.

  # Console

  1. `make impersonate org="org_id or org_name or org_slug"`
  2. Click the generated link
  """

  alias Sequin.Accounts
  alias Sequin.Error
  alias Sequin.Redis

  require Logger

  @type user_id :: Accounts.User.id() | Accounts.User.email()

  def getdel_secret(secret) do
    case Redis.command(["GETDEL", secret]) do
      {:ok, nil} ->
        {:error, Error.not_found(entity: :secret)}

      {:ok, value} ->
        [impersonating_user_id, impersonated_user_id] = String.split(value, ":")
        {:ok, %{impersonating_user_id: impersonating_user_id, impersonated_user_id: impersonated_user_id}}
    end
  end

  @spec generate_link(impersonating_user_id :: user_id(), impersonated_user_id :: user_id()) ::
          {:ok, String.t()} | {:error, Error.t()}
  def generate_link(impersonating_user_id, impersonated_user_id)
      when is_binary(impersonating_user_id) and is_binary(impersonated_user_id) do
    with {:ok, impersonating_user} <- find_user(impersonating_user_id),
         {:ok, impersonated_user} <- find_user(impersonated_user_id) do
      generate_link(impersonating_user, impersonated_user)
    else
      {:error, error} ->
        print("Error: #{Exception.message(error)}")

        {:error, error}
    end
  end

  @spec generate_link(impersonating_user :: Accounts.User.t(), impersonated_user :: Accounts.User.t()) ::
          {:ok, String.t()} | {:error, Error.t()}
  def generate_link(impersonating_user, impersonated_user) do
    secret = 48 |> :crypto.strong_rand_bytes() |> Base.encode32(padding: false)

    {:ok, _} =
      Redis.command(["SET", secret, "#{impersonating_user.id}:#{impersonated_user.id}", "PX", to_timeout(minute: 1)])

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

  defp find_user(user_id) do
    if Sequin.String.sequin_uuid?(user_id) do
      Accounts.get_user(user_id)
    else
      Accounts.get_user(email: user_id)
    end
  end

  if Mix.env() == :test do
    defp print(msg), do: Logger.info(msg)
  else
    # credo:disable-for-next-line
    defp print(msg), do: IO.puts(msg)
  end
end
