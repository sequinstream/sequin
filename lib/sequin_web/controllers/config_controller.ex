defmodule SequinWeb.ConfigController do
  use SequinWeb, :controller

  alias Sequin.Error
  alias Sequin.YamlLoader
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def plan_yaml(conn, %{"yaml" => yaml}) do
    account_id = conn.assigns.account_id

    with {:ok, config} <- parse_yaml(yaml),
         {:ok, changesets} <- YamlLoader.plan_from_config(account_id, config) do
      render(conn, "plan.json", changesets: changesets)
    end
  end

  def apply_yaml(conn, _params) do
    # account_id = conn.assigns.account_id

    conn
  end

  defp parse_yaml(yml) do
    case YamlElixir.read_from_string(yml) do
      {:ok, config} ->
        {:ok, config}

      {:error, error} ->
        {:error, Error.bad_request(message: "Error reading config file: #{inspect(error)}")}
    end
  end
end
