defmodule Sequin.Datadog do
  @moduledoc """
  Module for interacting with the Datadog Incident Management API.
  """
  alias Sequin.Datadog.Incident
  alias Sequin.Error

  require Logger

  @base_url "https://api.datadoghq.com/api/v2"

  @spec create_incident(Incident.t()) :: {:ok, Incident.t()} | {:error, Error.t()}
  def create_incident(%Incident{} = incident) do
    incident_attrs = incident |> Map.from_struct() |> Sequin.Map.reject_nil_values()

    payload = %{
      data: %{
        type: "incidents",
        id: incident.id,
        attributes: incident_attrs
      }
    }

    run_request(method: :post, url: "/incidents", json: payload)
  end

  @spec get_incident(String.t()) :: {:ok, Incident.t()} | {:error, Error.t()}
  def get_incident(incident_id) do
    run_request(method: :get, url: "/incidents/#{incident_id}")
  end

  @spec update_incident(String.t(), Incident.t()) :: {:ok, Incident.t()} | {:error, Error.t()}
  def update_incident(incident_id, %Incident{} = incident) do
    incident_attrs = incident |> Map.from_struct() |> Sequin.Map.reject_nil_values()

    payload = %{
      data: %{
        type: "incidents",
        id: incident_id,
        attributes: incident_attrs
      }
    }

    run_request(method: :patch, url: "/incidents/#{incident_id}", json: payload)
  end

  @spec delete_incident(String.t()) :: :ok | {:error, Error.t()}
  def delete_incident(incident_id) do
    run_request(method: :delete, url: "/incidents/#{incident_id}")
  end

  @spec list_incidents(map()) :: {:ok, %{data: [Incident.t()], page: map()}} | {:error, Error.t()}
  def list_incidents(params \\ %{}) do
    run_request(method: :get, url: "/incidents", params: params)
  end

  @spec search_incidents(String.t()) :: {:ok, %{data: [Incident.t()], page: map()}} | {:error, Error.t()}
  def search_incidents(query) do
    run_request(method: :get, url: "/incidents/search", params: %{"query" => query})
  end

  @spec list_all_incidents() :: {:ok, [Incident.t()]} | {:error, Error.t()}
  @spec list_all_incidents(list() | nil, String.t() | nil) :: {:ok, [Incident.t()]} | {:error, String.t()}
  def list_all_incidents(acc \\ [], offset \\ nil) do
    params =
      if offset do
        %{"page[size]" => 100, "page[offset]" => offset}
      else
        %{"page[size]" => 100}
      end

    case run_request(method: :get, url: "/incidents", params: params) do
      {:ok, %{data: data, page: %{"next_offset" => next_offset}}} ->
        list_all_incidents(acc ++ data, next_offset)

      {:ok, %{data: data}} ->
        {:ok, acc ++ data}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @headers [
    {"Content-Type", "application/json"},
    {"Accept", "application/json"}
  ]

  defp run_request(opts) do
    config = Application.get_env(:sequin, :datadog_req_opts)

    if config do
      config
      |> Keyword.put(:base_url, @base_url)
      |> Keyword.merge(opts)
      |> Keyword.update(:headers, @headers, fn headers ->
        Enum.concat(headers, @headers)
      end)
      |> Req.new()
      |> Req.Request.run_request()
      |> handle_response()
    else
      Logger.debug("Datadog API key not configured. Skipping request: #{inspect(opts)}")
      {:error, "Datadog API key not configured"}
    end
  end

  defp handle_response({_request, %Req.Response{status: 204}}) do
    :ok
  end

  defp handle_response({_request, %Req.Response{status: status, body: body}}) when status in 200..299 do
    {:ok, parse_response(body)}
  end

  defp handle_response({_request, %Req.Response{status: 404}}) do
    {:error, Error.not_found(entity: :incident)}
  end

  defp handle_response({_request, %Req.Response{status: status, body: body}}) do
    body =
      if Map.has_key?(body, "errors") do
        Enum.join(body["errors"], ", ")
      else
        body
      end

    {:error, Error.service(code: to_string(status), message: body, service: :datadog)}
  end

  defp handle_response({_request, exception}) do
    {:error, Error.service(code: "request_failed", message: inspect(exception), service: :datadog)}
  end

  defp parse_response(%{"data" => data, "meta" => %{"pagination" => pagination}}) do
    %{
      data: Enum.map(data, &parse_data/1),
      page: pagination
    }
  end

  defp parse_response(%{"data" => data}) when is_list(data) do
    Enum.map(data, &parse_data/1)
  end

  defp parse_response(%{"data" => data}) when is_map(data) do
    parse_data(data)
  end

  defp parse_response(response) do
    response
  end

  defp parse_data(%{"id" => id, "type" => "incidents", "attributes" => attrs}) do
    attrs
    |> Map.put("id", id)
    |> Incident.from_json()
  end
end
