defmodule Sequin.Sinks.Azure.EventHub do
  @moduledoc false
  import Sequin.Error.Guards, only: [is_error: 1]

  alias Sequin.Error

  @eventhub_data_plane_suffix ".servicebus.windows.net"

  defmodule Client do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :namespace, String.t(), enforce: true
      field :shared_access_key_name, String.t(), enforce: true
      field :shared_access_key, String.t(), enforce: true
      field :event_hub_name, String.t(), enforce: true
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
      event_hub_name: opts.event_hub_name,
      req_opts: req_opts
    }
  end

  @spec publish_messages(Client.t(), [map()]) :: :ok | {:error, any()}
  def publish_messages(%Client{} = client, messages) do
    path = "#{client.event_hub_name}/messages"
    encoded_messages = Enum.map(messages, &to_encoded_message/1)
    req = base_req(client, path, method: :post, json: encoded_messages)

    case Req.request(req) do
      {:ok, %{status: 201}} ->
        :ok

      error ->
        handle_error(error, "publish messages")
    end
  end

  @spec test_connection(Client.t()) :: :ok | {:error, any()}
  def test_connection(%Client{} = client) do
    path = client.event_hub_name
    req = base_req(client, path, method: :get)

    case Req.request(req) do
      {:ok, %{status: 200}} ->
        :ok

      error ->
        handle_error(error, "test connection")
    end
  end

  defp to_encoded_message(msg) do
    %{"Body" => Base.encode64(Jason.encode!(msg))}
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

  defp handle_error(error, req_desc) do
    case error do
      {:ok, %{status: 404}} ->
        {:error, Error.not_found(entity: :event_hub)}

      {:error, error} when is_error(error) ->
        {:error, error}

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
end
