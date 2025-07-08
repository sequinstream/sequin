defmodule Sequin.Sinks.Gcp.PubSub do
  @moduledoc false

  import Sequin.Error.Guards, only: [is_error: 1]

  alias Sequin.Error
  alias Sequin.Sinks.Gcp.Credentials

  require Logger

  @pubsub_base_url "https://pubsub.googleapis.com"

  # 55 minutes, tokens are valid for 1 hour
  @token_expiry_seconds 3300
  @pubsub_scope "https://www.googleapis.com/auth/pubsub"

  @cache_prefix "gcp_auth_token:"

  defmodule Client do
    @moduledoc false
    use TypedStruct

    alias Sequin.Sinks.Gcp.Credentials

    typedstruct do
      field :project_id, String.t(), enforce: true
      field :credentials, Credentials.t(), enforce: true
      field :auth_token, String.t()
      field :req_opts, keyword(), enforce: true
      field :use_emulator, boolean(), default: false
    end
  end

  def cache_prefix, do: @cache_prefix

  @doc """
  Creates a new PubSub client with the given project ID and credentials.
  Credentials should be a decoded Google service account JSON.
  """
  @spec new(String.t(), map(), keyword()) :: Client.t()
  def new(project_id, credentials, opts \\ []) do
    # Ensure the Credentials module is loaded, as it contains necessary atom keys
    Code.ensure_loaded(Sequin.Sinks.Gcp.Credentials)

    use_emulator = Keyword.get(opts, :use_emulator, false)
    req_opts = Keyword.merge(default_req_opts(), Keyword.get(opts, :req_opts, []))

    credentials =
      case credentials do
        %Credentials{} = creds ->
          creds

        map ->
          creds = Sequin.Map.atomize_keys(map)
          struct(Credentials, creds)
      end

    %Client{
      project_id: project_id,
      credentials: credentials,
      auth_token: nil,
      req_opts: req_opts,
      use_emulator: use_emulator
    }
  end

  @doc """
  Gets metadata about a Pub/Sub topic including statistics.
  Returns topic configuration and message count statistics.
  """
  @spec topic_metadata(Client.t(), String.t()) :: {:ok, map()} | {:error, Error.t()}
  def topic_metadata(%Client{} = client, topic_id) do
    path = topic_path(client.project_id, topic_id)

    case authenticated_request(client, :get, path) do
      {:ok, %{status: 200, body: body}} ->
        {:ok, parse_topic_metadata(body)}

      error ->
        handle_error(error, "get topic metadata")
    end
  end

  @doc """
  Publishes a batch of messages to a Pub/Sub topic.
  Messages should be a list of maps with :data and optional :attributes fields.
  """
  @spec publish_messages(Client.t(), String.t(), list(map())) :: :ok | {:error, Error.t()}
  def publish_messages(%Client{} = client, topic_id, messages) when is_list(messages) do
    path = "#{topic_path(client.project_id, topic_id)}:publish"

    encoded_messages =
      Enum.map(messages, fn msg ->
        %{
          "data" => Base.encode64(Jason.encode!(msg.data)),
          "attributes" => msg[:attributes] || %{},
          "orderingKey" => msg[:ordering_key]
        }
      end)

    payload = %{
      "messages" => encoded_messages
    }

    case authenticated_request(client, :post, path, json: payload) do
      {:ok, %{status: 200}} ->
        :ok

      error ->
        handle_error(error, "publish messages")
    end
  end

  @doc """
  Pulls messages from a Pub/Sub subscription.
  Returns a list of messages with their ack_ids.
  Options:
    * :max_messages - Maximum number of messages to return (default: 10)
    * :return_immediately - Whether to return immediately if no messages (default: true)
  """
  @spec pull_messages(Client.t(), String.t(), keyword()) :: {:ok, list(map())} | {:error, Error.t()}
  def pull_messages(%Client{} = client, subscription_id, opts \\ []) do
    path = "projects/#{client.project_id}/subscriptions/#{subscription_id}:pull"

    payload = %{
      "maxMessages" => Keyword.get(opts, :max_messages, 10),
      "returnImmediately" => Keyword.get(opts, :return_immediately, true)
    }

    case authenticated_request(client, :post, path, json: payload) do
      {:ok, %{status: 200, body: %{"receivedMessages" => messages}}} ->
        decoded_messages =
          Enum.map(messages, fn msg ->
            %{
              ack_id: msg["ackId"],
              data: msg["message"]["data"] |> Base.decode64!() |> Jason.decode!(),
              attributes: msg["message"]["attributes"] || %{},
              publish_time: msg["message"]["publishTime"]
            }
          end)

        {:ok, decoded_messages}

      {:ok, %{status: 200, body: %{}}} ->
        {:ok, []}

      error ->
        handle_error(error, "pull messages")
    end
  end

  @doc """
  Acknowledges messages in a subscription by their ack_ids.
  Takes a list of ack_ids and sends them to be acknowledged.
  """
  @spec ack_messages(Client.t(), String.t(), list(String.t())) :: :ok | {:error, Error.t()}
  def ack_messages(%Client{} = client, subscription_id, ack_ids) when is_list(ack_ids) do
    path = "projects/#{client.project_id}/subscriptions/#{subscription_id}:acknowledge"

    payload = %{
      "ackIds" => ack_ids
    }

    case authenticated_request(client, :post, path, json: payload) do
      {:ok, %{status: 200}} ->
        :ok

      error ->
        handle_error(error, "acknowledge messages")
    end
  end

  @doc """
  Tests the connection to GCP PubSub by attempting to delete a non-existent subscription.
  Returns :ok if the connection is working (based on receiving the expected 404 response),
  or an error if the connection fails.

  This request is supported on both GCP Cloud and the emulator.
  """
  @spec test_connection(Client.t()) :: :ok | {:error, Error.t()}
  def test_connection(%Client{} = client) do
    test_sub_id = "sequin-test-#{UUID.uuid4()}"
    path = "projects/#{client.project_id}/subscriptions/#{test_sub_id}"

    case authenticated_request(client, :delete, path) do
      {:ok, %{status: 404, body: %{"error" => %{"status" => "NOT_FOUND"}}}} ->
        :ok

      error ->
        handle_error(error, "test connection")
    end
  end

  # Private helpers

  defp handle_error(error, req_desc) do
    case error do
      {:ok, %{status: 404, body: %{"error" => %{"message" => message}}}} ->
        {:error, Error.invariant(code: :not_found, message: "#{req_desc} failed: #{message}")}

      {:ok, %{status: 404}} ->
        {:error, Error.not_found(entity: :pubsub_topic)}

      {:error, error} when is_error(error) ->
        {:error, error}

      {:ok, %{body: %{"error" => error}}} ->
        {:error,
         Error.service(
           service: :gcp_pubsub,
           message: "Request failed: #{req_desc}, error: #{inspect(error)}",
           details: error
         )}

      {:ok, res} ->
        {:error,
         Error.service(
           service: :gcp_pubsub,
           message: "Request failed: #{req_desc}, bad status (status=#{res.status})",
           details: res
         )}

      {:error, error} when is_exception(error) ->
        {:error,
         Error.service(
           service: :gcp_pubsub,
           message: "Request failed: #{req_desc} with error: #{Exception.message(error)}",
           details: error
         )}

      {:error, error} ->
        {:error,
         Error.service(
           service: :gcp_pubsub,
           message: "Request failed: #{req_desc} with error: #{inspect(error)}",
           details: error
         )}
    end
  end

  defp authenticated_request(%Client{} = client, method, path, opts \\ []) do
    base_url = Keyword.get(client.req_opts, :base_url, @pubsub_base_url)

    headers = maybe_put_auth_headers([{"content-type", "application/json"}], client)

    case headers do
      {:ok, headers} ->
        [
          method: method,
          url: "#{base_url}/v1/#{path}",
          headers: headers,
          compress_body: true
        ]
        |> Req.new()
        |> Req.merge(client.req_opts)
        |> Req.merge(opts)
        |> Req.request()

      error ->
        error
    end
  end

  # Blank credentials means we are using the emulator
  defp maybe_put_auth_headers(headers, %Client{use_emulator: true}), do: {:ok, headers}

  defp maybe_put_auth_headers(headers, %Client{} = client) do
    case ensure_auth_token(client) do
      {:ok, token} -> {:ok, [{"authorization", "Bearer #{token}"} | headers]}
      error -> error
    end
  end

  defp ensure_auth_token(%Client{} = client) do
    cache_key = @cache_prefix <> cache_key(client.credentials)

    case ConCache.get(Sequin.Cache, cache_key) do
      nil ->
        generate_and_cache_token(client, cache_key)

      token ->
        {:ok, token}
    end
  end

  # Hash the credentials to generate a (safe) cache key
  def cache_key(credentials) do
    credentials
    |> Map.from_struct()
    |> Map.take([:client_email, :private_key])
    |> Jason.encode!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp generate_and_cache_token(client, cache_key) do
    with {:ok, token} <- generate_jwt_token(client) do
      # Store with TTL slightly less than token expiry
      ConCache.put(Sequin.Cache, cache_key, %ConCache.Item{
        value: token,
        ttl: to_timeout(second: @token_expiry_seconds)
      })

      {:ok, token}
    end
  end

  defp generate_jwt_token(%Client{credentials: %Credentials{} = credentials} = client) do
    now = System.system_time(:second)

    claims = %{
      "iss" => credentials.client_email,
      "scope" => @pubsub_scope,
      "aud" => "https://oauth2.googleapis.com/token",
      "exp" => now + 3600,
      "iat" => now
    }

    with {:ok, jwt} <- sign_jwt(claims, credentials.private_key) do
      exchange_jwt_for_token(jwt, client.req_opts)
    end
  end

  @spec sign_jwt(map(), String.t()) :: {:ok, String.t()}
  defp sign_jwt(claims, private_key) do
    # Parse the PEM formatted private key
    jwk = JOSE.JWK.from_pem(private_key)

    # Create the JWT with header and claims
    jws = %{"alg" => "RS256"}
    jwt = JOSE.JWT.from_map(claims)

    # Sign the JWT and convert to compact form
    {_, signed} = JOSE.JWT.sign(jwk, jws, jwt)
    compact = signed |> JOSE.JWS.compact() |> elem(1)
    {:ok, compact}
  rescue
    # Will happen with a bad key
    _e ->
      {:error,
       Error.service(
         service: :gcp_pubsub,
         message: "Failed to sign JWT. Credentials may be invalid."
       )}
  end

  defp exchange_jwt_for_token(jwt, req_opts) do
    body =
      URI.encode_query(%{
        grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
        assertion: jwt
      })

    headers = [
      {"content-type", "application/x-www-form-urlencoded"}
    ]

    req =
      [base_url: "https://oauth2.googleapis.com/token", headers: headers, body: body, method: :post]
      |> Req.new()
      |> Req.merge(req_opts)

    case Req.request(req) do
      {:ok, %{status: 200, body: %{"access_token" => token}}} ->
        {:ok, token}

      {:ok, %{status: status, body: body}} ->
        {:error,
         Error.service(
           service: :gcp_pubsub,
           message: "Failed to exchange JWT for access token",
           details: %{status: status, body: body}
         )}

      {:error, _} = error ->
        error
    end
  end

  defp topic_path(project_id, topic_id) do
    "projects/#{project_id}/topics/#{topic_id}"
  end

  defp parse_topic_metadata(body) do
    # Everything but `name` is optional in response
    %{"name" => name} = body

    %{
      name: name,
      labels: Map.get(body, "labels", %{}),
      storage_policy: Map.get(body, "messageStoragePolicy"),
      kms_key: Map.get(body, "kmsKeyName"),
      schema_settings: Map.get(body, "schemaSettings"),
      message_retention: Map.get(body, "messageRetentionDuration")
    }
  end

  defp default_req_opts do
    Application.get_env(:sequin, :gcp_pubsub, [])[:req_opts] || []
  end
end
