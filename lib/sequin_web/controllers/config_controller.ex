defmodule SequinWeb.ConfigController do
  use SequinWeb, :controller

  alias Sequin.Accounts.Account
  alias Sequin.Accounts.User
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.HttpPullConsumer
  alias Sequin.Consumers.HttpPushConsumer
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.Sequence
  alias Sequin.YamlLoader
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def apply(conn, %{"yml" => yml}) do
    account_id = conn.assigns.account_id

    case YamlLoader.apply_from_yml(account_id, yml) do
      {:ok, {:ok, resources}} ->
        json(conn, resources)

      {:ok, {:error, error}} ->
        {:error, error}

      {:error, error} ->
        {:error, error}
    end
  end

  def plan(conn, %{"yml" => yml}) do
    account_id = conn.assigns.account_id

    case YamlLoader.plan_from_yml(account_id, yml) do
      {:ok, planned_resources, current_resources} ->
        envelopes = diff_resources(planned_resources, current_resources)
        json(conn, envelopes)

      {:error, error} ->
        {:error, error}
    end
  end

  defp diff_resources(planned_resources, current_resources) do
    creates =
      planned_resources
      |> Enum.reject(fn planned_resource ->
        Enum.any?(current_resources, fn current_resource ->
          same_resource?(current_resource, planned_resource)
        end)
      end)
      |> Enum.map(fn resource ->
        %{
          resource_type: get_resource_type(resource),
          action: "create",
          new: resource,
          old: nil
        }
      end)

    deletes =
      current_resources
      |> Enum.reject(fn current_resource ->
        Enum.any?(planned_resources, fn planned_resource -> same_resource?(current_resource, planned_resource) end)
      end)
      |> Enum.map(fn resource ->
        %{
          resource_type: get_resource_type(resource),
          action: "delete",
          new: nil,
          old: resource
        }
      end)

    updates =
      current_resources
      |> Enum.filter(fn current_resource ->
        Enum.any?(planned_resources, fn planned_resource -> same_resource?(current_resource, planned_resource) end)
      end)
      |> Enum.map(fn current_resource ->
        planned_resource =
          Enum.find(planned_resources, fn planned_resource -> same_resource?(current_resource, planned_resource) end)

        %{
          resource_type: get_resource_type(current_resource),
          action: "update",
          new: planned_resource,
          old: current_resource
        }
      end)

    creates ++ updates ++ deletes
  end

  defp get_resource_type(%User{}), do: "user"
  defp get_resource_type(%Account{}), do: "account"
  defp get_resource_type(%PostgresDatabase{}), do: "postgres_database"
  defp get_resource_type(%Sequence{}), do: "sequence"
  defp get_resource_type(%HttpEndpoint{}), do: "http_endpoint"
  defp get_resource_type(%HttpPullConsumer{}), do: "consumer_group"
  defp get_resource_type(%HttpPushConsumer{}), do: "webhook_subscription"

  defp same_resource?(%User{email: email}, %User{email: email}), do: true
  defp same_resource?(%Account{name: name}, %Account{name: name}), do: true
  defp same_resource?(%PostgresDatabase{name: name}, %PostgresDatabase{name: name}), do: true
  defp same_resource?(%Sequence{name: name}, %Sequence{name: name}), do: true
  defp same_resource?(%HttpEndpoint{name: name}, %HttpEndpoint{name: name}), do: true
  defp same_resource?(%HttpPullConsumer{name: name}, %HttpPullConsumer{name: name}), do: true
  defp same_resource?(%HttpPushConsumer{name: name}, %HttpPushConsumer{name: name}), do: true
  defp same_resource?(_, _), do: false
end
