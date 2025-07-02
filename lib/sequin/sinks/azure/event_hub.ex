defmodule Sequin.Sinks.Azure.EventHub do
  @moduledoc false
  import Sequin.Error.Guards, only: [is_error: 1]

  alias Sequin.Error
  alias Sequin.Error.NotFoundError

  @eventhub_data_plane_suffix ".servicebus.windows.net"

  defmodule Client do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :namespace, String.t(), enforce: true
      field :shared_access_key_name, String.t(), enforce: true
      field :shared_access_key, String.t(), enforce: true
      field :req_opts, keyword(), enforce: true
    end
  end

  @spec new(map(), keyword()) :: Client.t()
  def new(opts, req_opts \\ []) do
    req_opts = Keyword.merge(default_req_opts(), req_opts)

    %Client{
      namespace: opts.namespace,
      shared_access_key_name: opts.shared_access_key_name,
      shared_access_key: opts.shared_access_key,
      req_opts: req_opts
    }
  end

  @spec publish_messages(Client.t(), String.t(), [map()]) :: :ok | {:error, any()}
  def publish_messages(%Client{} = client, event_hub_name, messages) do
    path = "#{event_hub_name}/messages"
    encoded_messages = Enum.map(messages, &to_encoded_message/1)
    req = base_req(client, path, method: :post, json: encoded_messages)

    case Req.request(req) do
      {:ok, %{status: 201}} ->
        :ok

      error ->
        handle_error(client, error, "publish messages")
    end
  end

  @doc """
  Tests if the connection to the Event Hub is valid. This tests for a valid namespace and auth token.

  We choose a non-deleterious operation that is OK with limited (send-only) permissions.
  """
  @spec test_connection(Client.t(), String.t()) :: :ok | {:error, any()}
  def test_connection(%Client{} = client, event_hub_name) do
    path = "#{event_hub_name}/does-not-exist"
    req = base_req(client, path, method: :get)

    case Req.request(req) do
      {:ok, %{status: 400, body: body}} = error ->
        if body =~ "The requested HTTP operation is not supported in an EventHub" do
          :ok
        else
          handle_error(client, error, "test connection")
        end

      # This means the namespace is vaild, but not the event hub name
      {:ok, %{status: 200}} ->
        {:error,
         Error.not_found(
           entity: :event_hub,
           params: %{namespace: client.namespace, event_hub_name: event_hub_name}
         )}

      error ->
        handle_error(client, error, "test connection")
    end
  end

  @doc """
  Test credentials and permissions without requiring a specific Event Hub. Useful for dynamic routing.
  """
  @spec test_credentials_and_permissions(Client.t()) :: :ok | {:error, any()}
  def test_credentials_and_permissions(%Client{} = client) do
    case test_connection(client, "does-not-exist") do
      {:error, %NotFoundError{entity: :event_hub}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp to_encoded_message(msg) do
    %{"Body" => Jason.encode!(msg)}
  end

  defp base_req(client, path, opts) do
    {headers, opts} = Keyword.pop(opts, :headers, [])
    full_path = "#{client.namespace}#{@eventhub_data_plane_suffix}/#{path}"
    sas_token = generate_sas_token(client, full_path)
    headers = [{"authorization", sas_token}, {"content-type", "application/vnd.microsoft.servicebus.json"}] ++ headers

    [url: "https://#{full_path}", headers: headers]
    |> Keyword.merge(opts)
    |> Req.new()
    |> Req.merge(client.req_opts)
  end

  defp generate_sas_token(client, path) do
    uri = URI.encode_www_form("https://#{path}")
    expire_in = 3600
    expiry = System.system_time(:second) + expire_in
    data = "#{uri}\n#{expiry}"

    signature =
      :hmac
      |> :crypto.mac(:sha256, client.shared_access_key, data)
      |> Base.encode64()
      |> URI.encode_www_form()

    "SharedAccessSignature sr=#{uri}&sig=#{signature}&se=#{expiry}&skn=#{client.shared_access_key_name}"
  end

  defp default_req_opts do
    :sequin
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:req_opts, [])
  end

  defp handle_error(%Client{} = client, error, req_desc) do
    case error do
      {:error, %Req.TransportError{reason: :nxdomain}} ->
        {:error,
         Error.not_found(
           entity: :event_hub,
           params: %{namespace: client.namespace}
         )}

      {:error, error} when is_error(error) ->
        {:error, error}

      {:ok, %Req.Response{body: body}} when is_binary(body) ->
        {:error,
         Error.service(
           service: :event_hub,
           message: "#{req_desc}: #{body}"
         )}

      {:ok, %Req.Response{} = res} ->
        {:error,
         Error.service(
           service: :azure_event_hub,
           message: "Request failed: #{req_desc}, bad status (status=#{res.status})",
           details: Map.from_struct(res)
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
end
