defmodule SequinWeb.YamlController do
  use SequinWeb, :controller

  alias Sequin.Accounts.Account
  alias Sequin.Accounts.User
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Databases.Sequence
  alias Sequin.Replication.WalPipeline
  alias Sequin.Transforms
  alias Sequin.YamlLoader
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def apply(conn, %{"yaml" => yaml}) do
    account_id = conn.assigns.account_id

    case YamlLoader.apply_from_yml(account_id, yaml) do
      {:ok, {:ok, resources}} ->
        json(conn, %{resources: resources})

      {:ok, {:error, error}} ->
        {:error, error}

      {:error, error} ->
        {:error, error}
    end
  end

  def plan(conn, %{"yaml" => yaml}) do
    account_id = conn.assigns.account_id

    case YamlLoader.plan_from_yml(account_id, yaml) do
      {:ok, planned_resources, current_resources} ->
        envelopes = diff_resources(planned_resources, current_resources)
        json(conn, %{changes: envelopes})

      {:error, error} ->
        {:error, error}
    end
  end

  def export(conn, _params) do
    yaml =
      conn.assigns.account_id
      |> YamlLoader.all_resources()
      |> Enum.group_by(&get_resource_type/1)
      # Skip support at this time
      |> Enum.reject(fn {resource_type, _resources} -> resource_type in ["account", "user"] end)
      |> Map.new(fn {resource_type, resources} ->
        {"#{resource_type}s", Enum.map(resources, &Transforms.to_external/1)}
      end)
      |> Ymlr.document!()

    json(conn, %{yaml: yaml})
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
          new: Transforms.to_external(resource),
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
          old: Transforms.to_external(resource)
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
          new: Transforms.to_external(planned_resource),
          old: Transforms.to_external(current_resource)
        }
      end)

    creates ++ updates ++ deletes
  end

  defp get_resource_type(%Account{}), do: "account"
  defp get_resource_type(%HttpEndpoint{}), do: "http_endpoint"
  defp get_resource_type(%SinkConsumer{}), do: "webhook_subscription"
  defp get_resource_type(%PostgresDatabase{}), do: "database"
  defp get_resource_type(%Sequence{}), do: "stream"
  defp get_resource_type(%User{}), do: "user"
  defp get_resource_type(%WalPipeline{}), do: "change_capture_pipeline"

  defp same_resource?(%Account{name: name}, %Account{name: name}), do: true
  defp same_resource?(%HttpEndpoint{name: name}, %HttpEndpoint{name: name}), do: true
  defp same_resource?(%SinkConsumer{name: name}, %SinkConsumer{name: name}), do: true
  defp same_resource?(%PostgresDatabase{name: name}, %PostgresDatabase{name: name}), do: true
  defp same_resource?(%Sequence{name: name}, %Sequence{name: name}), do: true
  defp same_resource?(%User{email: email}, %User{email: email}), do: true
  defp same_resource?(%WalPipeline{name: name}, %WalPipeline{name: name}), do: true
  defp same_resource?(_, _), do: false
end
