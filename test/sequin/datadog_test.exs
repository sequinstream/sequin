defmodule Sequin.DatadogTest do
  use ExUnit.Case, async: true

  alias Sequin.Datadog
  alias Sequin.Datadog.Incident
  alias Sequin.Factory.Datadog, as: Factory

  setup do
    Req.Test.stub(Datadog, fn req ->
      case {req.method, req.request_path} do
        {"POST", "/api/v2/incidents"} ->
          incident = Factory.incident()
          Req.Test.json(req, %{"data" => incident_response(incident)})

        {"GET", "/api/v2/incidents/123"} ->
          incident = Factory.incident(%{id: "123"})
          Req.Test.json(req, %{"data" => incident_response(incident)})

        {"PATCH", "/api/v2/incidents/123"} ->
          incident = Factory.incident(%{id: "123", title: "Updated Incident", status: "resolved"})
          Req.Test.json(req, %{"data" => incident_response(incident)})

        {"DELETE", "/api/v2/incidents/123"} ->
          Plug.Conn.resp(req, 204, "")

        {"GET", "/api/v2/incidents"} ->
          incidents = [Factory.incident(%{id: "123"})]
          Req.Test.json(req, %{"data" => Enum.map(incidents, &incident_response/1)})

        {"GET", "/api/v2/incidents/search"} ->
          incidents = [Factory.incident(%{id: "123"})]
          Req.Test.json(req, %{"data" => Enum.map(incidents, &incident_response/1)})
      end
    end)

    :ok
  end

  test "create_incident" do
    incident = Factory.incident()
    assert {:ok, %Incident{id: id}} = Datadog.create_incident(incident)
    assert is_binary(id)
  end

  test "get_incident" do
    assert {:ok, %Incident{id: "123"}} = Datadog.get_incident("123")
  end

  test "update_incident" do
    incident = Factory.incident(%{title: "Updated Incident"})
    assert {:ok, %Incident{id: "123", title: "Updated Incident"}} = Datadog.update_incident("123", incident)
  end

  test "delete_incident" do
    assert :ok = Datadog.delete_incident("123")
  end

  test "list_incidents" do
    assert {:ok, [%Incident{id: "123"}]} = Datadog.list_incidents()
  end

  test "search_incidents" do
    assert {:ok, [%Incident{id: "123"}]} = Datadog.search_incidents("test query")
  end

  defp incident_response(incident) do
    attrs = incident |> Map.from_struct() |> Map.delete(:id)

    %{
      "id" => incident.id,
      "type" => "incidents",
      "attributes" => attrs
    }
  end
end
