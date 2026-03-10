defmodule Sequin.Aws.IrsaCredentials do
  @moduledoc """
  GenServer that manages IRSA (IAM Roles for Service Accounts) credentials for EKS.

  Reads the projected JWT from `AWS_WEB_IDENTITY_TOKEN_FILE`, calls
  `STS:AssumeRoleWithWebIdentity` via `ex_aws_sts`, and refreshes the resulting
  temporary credentials before they expire.

  The GenServer only starts when both `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE`
  environment variables are set. Otherwise it returns `:ignore` from `init/1`.
  """

  use GenServer

  alias Sequin.Error

  require Logger

  @default_session_name "sequin-irsa"
  # Refresh at 80% of credential TTL
  @refresh_ratio 0.8
  # Minimum refresh interval: 60 seconds
  @min_refresh_ms to_timeout(minute: 1)
  # Retry interval on failure: 30 seconds
  @retry_ms to_timeout(second: 30)

  defmodule State do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :role_arn, String.t(), enforce: true
      field :token_file, String.t(), enforce: true
      field :session_name, String.t(), enforce: true
      field :credentials, map()
      field :expires_at, DateTime.t()
      field :refresh_timer, reference()
    end
  end

  # -- Public API --

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns true if the IRSA GenServer is running and has credentials available.
  """
  @spec available?() :: boolean()
  def available? do
    case GenServer.whereis(__MODULE__) do
      nil -> false
      pid -> Process.alive?(pid) and GenServer.call(pid, :available?)
    end
  catch
    :exit, _ -> false
  end

  @doc """
  Returns the current IRSA credentials.

  Returns `{:ok, %{access_key_id: ..., secret_access_key: ..., token: ...}}`
  or `{:error, reason}`.
  """
  @spec get_credentials() :: {:ok, map()} | {:error, Error.t()}
  def get_credentials do
    GenServer.call(__MODULE__, :get_credentials)
  catch
    :exit, _ ->
      {:error, Error.service(service: :aws, message: "IRSA credentials server not available")}
  end

  # -- GenServer callbacks --

  @impl GenServer
  def init(opts) do
    role_arn = Keyword.get(opts, :role_arn) || System.get_env("AWS_ROLE_ARN")
    token_file = Keyword.get(opts, :token_file) || System.get_env("AWS_WEB_IDENTITY_TOKEN_FILE")
    session_name = Keyword.get(opts, :session_name) || System.get_env("AWS_ROLE_SESSION_NAME", @default_session_name)

    if is_nil(role_arn) or is_nil(token_file) do
      :ignore
    else
      state = %State{
        role_arn: role_arn,
        token_file: token_file,
        session_name: session_name
      }

      # Fetch credentials immediately on startup
      case fetch_credentials(state) do
        {:ok, new_state} ->
          {:ok, new_state}

        {:error, reason} ->
          Logger.error("IRSA initial credential fetch failed: #{inspect(reason)}. Will retry.")
          timer = Process.send_after(self(), :refresh, @retry_ms)
          {:ok, %{state | refresh_timer: timer}}
      end
    end
  end

  @impl GenServer
  def handle_call(:available?, _from, %State{credentials: credentials} = state) do
    {:reply, not is_nil(credentials), state}
  end

  @impl GenServer
  def handle_call(:get_credentials, _from, %State{credentials: nil} = state) do
    {:reply, {:error, Error.service(service: :aws, message: "IRSA credentials not yet available")}, state}
  end

  @impl GenServer
  def handle_call(:get_credentials, _from, %State{credentials: credentials} = state) do
    {:reply, {:ok, credentials}, state}
  end

  @impl GenServer
  def handle_info(:refresh, state) do
    case fetch_credentials(state) do
      {:ok, new_state} ->
        {:noreply, new_state}

      {:error, reason} ->
        Logger.error("IRSA credential refresh failed: #{inspect(reason)}. Will retry in #{@retry_ms}ms.")
        timer = Process.send_after(self(), :refresh, @retry_ms)
        {:noreply, %{state | refresh_timer: timer}}
    end
  end

  # -- Private --

  defp fetch_credentials(%State{} = state) do
    with {:ok, token} <- read_token_file(state.token_file),
         {:ok, result} <- assume_role_with_web_identity(state.role_arn, token, state.session_name) do
      credentials = %{
        access_key_id: result.access_key_id,
        secret_access_key: result.secret_access_key,
        token: result.session_token
      }

      expires_at = result.expiration
      refresh_ms = calculate_refresh_ms(expires_at)

      if state.refresh_timer, do: Process.cancel_timer(state.refresh_timer)
      timer = Process.send_after(self(), :refresh, refresh_ms)

      Logger.info("IRSA credentials fetched successfully. Next refresh in #{div(refresh_ms, 1000)}s.")

      {:ok, %{state | credentials: credentials, expires_at: expires_at, refresh_timer: timer}}
    end
  end

  defp read_token_file(path) do
    case File.read(path) do
      {:ok, token} ->
        {:ok, String.trim(token)}

      {:error, reason} ->
        {:error, Error.service(service: :aws, message: "Failed to read IRSA token file #{path}: #{inspect(reason)}")}
    end
  end

  defp assume_role_with_web_identity(role_arn, token, session_name) do
    op =
      ExAws.STS.assume_role_with_web_identity(
        role_arn,
        session_name,
        token,
        duration: 3600
      )

    # AssumeRoleWithWebIdentity doesn't require pre-existing AWS credentials —
    # the web identity token IS the authentication. We pass dummy credentials
    # to satisfy ExAws's credential resolution (same approach as the built-in
    # AssumeRoleWebIdentityAdapter).
    config = %{access_key_id: "not-used", secret_access_key: "not-used"}

    case ExAws.request(op, config) do
      {:ok, %{body: body}} ->
        parse_assume_role_response(body)

      {:error, {:http_error, status, %{body: body}}} ->
        {:error, Error.service(service: :aws, message: "STS AssumeRoleWithWebIdentity failed (HTTP #{status}): #{body}")}

      {:error, reason} ->
        {:error, Error.service(service: :aws, message: "STS AssumeRoleWithWebIdentity failed: #{inspect(reason)}")}
    end
  end

  defp parse_assume_role_response(body) do
    credentials = body[:credentials]

    if credentials do
      expiration =
        case DateTime.from_iso8601(credentials[:expiration]) do
          {:ok, dt, _offset} -> dt
          _ -> DateTime.add(DateTime.utc_now(), 3600, :second)
        end

      {:ok,
       %{
         access_key_id: credentials[:access_key_id],
         secret_access_key: credentials[:secret_access_key],
         session_token: credentials[:session_token],
         expiration: expiration
       }}
    else
      {:error, Error.service(service: :aws, message: "Unexpected STS response format: #{inspect(body)}")}
    end
  end

  defp calculate_refresh_ms(expires_at) do
    now = DateTime.utc_now()
    ttl_seconds = DateTime.diff(expires_at, now, :second)
    refresh_seconds = trunc(ttl_seconds * @refresh_ratio)
    max(refresh_seconds * 1000, @min_refresh_ms)
  end
end
