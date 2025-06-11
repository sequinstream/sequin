defmodule SequinWeb.YamlController do
  use SequinWeb, :controller

  alias Sequin.Accounts.Account
  alias Sequin.Accounts.User
  alias Sequin.Consumers.Function
  alias Sequin.Consumers.HttpEndpoint
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Posthog
  alias Sequin.Replication.WalPipeline
  alias Sequin.Transforms
  alias Sequin.YamlLoader
  alias SequinWeb.ApiFallbackPlug

  action_fallback ApiFallbackPlug

  def apply(conn, %{"yaml" => yaml}) do
    account_id = conn.assigns.account_id

    Posthog.capture("YAML Apply", %{
      distinct_id: "00000000-0000-0000-0000-000000000000",
      properties: %{
        "$groups": %{account: account_id}
      }
    })

    case YamlLoader.apply_from_yml(account_id, yaml) do
      {:ok, resources} ->
        Posthog.capture("YAML Applied", %{
          distinct_id: "00000000-0000-0000-0000-000000000000",
          properties: %{
            resource_count: length(resources),
            resource_types: resources |> Enum.map(&get_resource_type/1) |> Enum.uniq(),
            "$groups": %{account: account_id}
          }
        })

        json(conn, %{resources: resources})

      {:error, error} ->
        {:error, error}
    end
  end

  def plan(conn, %{"yaml" => yaml}) do
    account_id = conn.assigns.account_id

    Posthog.capture("YAML Plan", %{
      distinct_id: "00000000-0000-0000-0000-000000000000",
      properties: %{
        "$groups": %{account: account_id}
      }
    })

    case YamlLoader.plan_from_yml(account_id, yaml) do
      {:ok, planned_resources, current_resources, actions} ->
        envelopes = diff_resources(planned_resources, current_resources)

        Posthog.capture("YAML Planned", %{
          distinct_id: "00000000-0000-0000-0000-000000000000",
          properties: %{
            changes_count: length(envelopes),
            creates: Enum.count(envelopes, &(&1.action == "create")),
            updates: Enum.count(envelopes, &(&1.action == "update")),
            deletes: Enum.count(envelopes, &(&1.action == "delete")),
            "$groups": %{account: account_id}
          }
        })

        json(conn, %{changes: envelopes, actions: actions})

      {:error, error} ->
        {:error, error}
    end
  end

  def export(conn, params) do
    account_id = conn.assigns.account_id
    show_sensitive = params["show-sensitive"] == "true"

    Posthog.capture("YAML Export", %{
      distinct_id: "00000000-0000-0000-0000-000000000000",
      properties: %{
        "$groups": %{account: account_id},
        show_sensitive: show_sensitive
      }
    })

    resources = YamlLoader.all_resources(account_id)
    resource_types = resources |> Enum.map(&get_resource_type/1) |> Enum.uniq()

    yaml =
      resources
      |> Enum.group_by(&get_resource_type/1)
      |> Enum.reject(fn {resource_type, _resources} -> resource_type in ["account", "user"] end)
      |> Map.new(fn {resource_type, resources} ->
        external_resources =
          resources
          |> Enum.map(&Transforms.to_external(&1, show_sensitive))
          |> Enum.map(&Map.delete(&1, :id))

        {"#{resource_type}s", external_resources}
      end)
      |> Ymlr.document!()

    Posthog.capture("YAML Exported", %{
      distinct_id: "00000000-0000-0000-0000-000000000000",
      properties: %{
        resource_count: length(resources),
        resource_types: resource_types,
        "$groups": %{account: account_id}
      }
    })

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
  defp get_resource_type(%SinkConsumer{}), do: "sink"
  defp get_resource_type(%PostgresDatabase{}), do: "database"
  defp get_resource_type(%User{}), do: "user"
  defp get_resource_type(%WalPipeline{}), do: "change_retention"
  defp get_resource_type(%Function{}), do: "function"

  defp same_resource?(%Account{name: name}, %Account{name: name}), do: true
  defp same_resource?(%HttpEndpoint{name: name}, %HttpEndpoint{name: name}), do: true
  defp same_resource?(%SinkConsumer{name: name}, %SinkConsumer{name: name}), do: true
  defp same_resource?(%PostgresDatabase{name: name}, %PostgresDatabase{name: name}), do: true
  defp same_resource?(%User{email: email}, %User{email: email}), do: true
  defp same_resource?(%WalPipeline{name: name}, %WalPipeline{name: name}), do: true
  defp same_resource?(%Function{name: name}, %Function{name: name}), do: true
  defp same_resource?(_, _), do: false
end
