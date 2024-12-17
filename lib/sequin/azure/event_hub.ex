defmodule Sequin.Azure.EventHub do
  @moduledoc false

  import Sequin.Error.Guards, only: [is_error: 1]

  alias Sequin.Error

  require Logger

  @eventhub_data_plane_suffix ".servicebus.windows.net"

  defmodule Client do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :subscription_id, String.t(), enforce: true
      field :resource_group, String.t(), enforce: true
      field :namespace, String.t(), enforce: true
      field :shared_access_key_name, String.t(), enforce: true
      field :shared_access_key, String.t(), enforce: true
      field :req_opts, keyword(), enforce: true
    end
  end

  @doc """
  Creates a new EventHub client with the given configuration.
  """
  @spec new(map(), keyword()) :: Client.t()
  def new(config, req_opts \\ []) do
    req_opts = Keyword.merge(default_req_opts(), req_opts)

    %Client{
      subscription_id: config.subscription_id,
      resource_group: config.resource_group,
      namespace: config.namespace,
      shared_access_key_name: config.shared_access_key_name,
      shared_access_key: config.shared_access_key,
      req_opts: req_opts
    }
  end

  @doc """
  Tests the connection to Event Hub by attempting to get hub properties using data plane API.
  This uses the same authentication and endpoint as publish_messages.
  """
  @spec test_connection(Client.t(), String.t()) :: :ok | {:error, Error.t()}
  def test_connection(%Client{} = client, event_hub_name) do
    path = "#{client.namespace}#{@eventhub_data_plane_suffix}/#{event_hub_name}"

    client
    |> base_req(path, method: :get)
    |> Req.request()
    |> case do
      {:ok, %{status: 200}} -> :ok
      {:ok, %{status: 201}} -> :ok
      error -> handle_error(error, "test connection")
    end
  end

  @doc """
  Publishes a batch of messages to an Event Hub.
  Messages should be a list of maps with :body and optional :properties fields.
  """
  @spec publish_messages(Client.t(), String.t(), list(map())) :: :ok | {:error, Error.t()}
  def publish_messages(%Client{} = client, event_hub_name, messages) when is_list(messages) do
    path = "#{event_hub_name}/messages"

    encoded_messages =
      Enum.map(messages, fn msg ->
        %{
          "Body" => Base.encode64(Jason.encode!(msg.body)),
          "Properties" => msg[:properties] || %{}
        }
      end)

    client
    |> base_req(path, method: :post, json: encoded_messages)
    |> Req.request()
    |> case do
      {:ok, %{status: status}} when status in [200, 201] ->
        :ok

      error ->
        handle_error(error, "publish messages")
    end
  end

  # Private helpers

  defp handle_error(error, req_desc) do
    case error do
      {:ok, %{status: 404} = resp} ->
        Logger.error("Event hub not found: #{inspect(resp)}")
        {:error, Error.not_found(entity: :event_hub)}

      {:error, error} when is_error(error) ->
        {:error, error}

      {:ok, %{body: %{"error" => error}}} ->
        {:error,
         Error.service(
           service: :azure_event_hub,
           message: "Request failed: #{req_desc}, error: #{inspect(error)}",
           details: error
         )}

      {:ok, res} ->
        {:error,
         Error.service(
           service: :azure_event_hub,
           message: "Request failed: #{req_desc}, bad status (status=#{res.status})",
           details: res
         )}

      {:error, error} ->
        {:error,
         Error.service(
           service: :azure_event_hub,
           message: "Request failed: #{req_desc}",
           details: error
         )}
    end
  end

  # Data plane requests (using SAS token auth)
  defp base_req(client, path, opts) do
    {headers, opts} = Keyword.pop(opts, :headers, [])
    full_path = "#{client.namespace}#{@eventhub_data_plane_suffix}/#{path}"
    sas_token = generate_sas_token(client, full_path)

    headers = [{"authorization", sas_token}, {"content-type", "application/json"}] ++ headers

    [url: "https://#{full_path}", headers: headers]
    |> Keyword.merge(opts)
    |> Req.new()
    |> Req.merge(client.req_opts)
  end

  defp generate_sas_token(client, path) do
    uri = URI.encode_www_form("https://#{path}")
    expiry = System.system_time(:second) + 3600

    string_to_sign = "#{uri}\n#{expiry}"

    signature =
      :hmac
      |> :crypto.mac(:sha256, client.shared_access_key, string_to_sign)
      |> Base.encode64()
      |> URI.encode_www_form()

    "SharedAccessSignature " <>
      "sr=#{uri}&" <>
      "sig=#{signature}&" <>
      "se=#{expiry}&" <>
      "skn=#{client.shared_access_key_name}"
  end

  defp default_req_opts do
    Application.get_env(:sequin, :azure_event_hub, [])[:req_opts] || []
  end
end
