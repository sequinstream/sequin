defmodule Sequin.Gcp.ApplicationDefaultCredentials do
  @moduledoc """
  Handles GCP Application Default Credentials (ADC) resolution using Goth.

  This module provides functionality to automatically discover and use GCP credentials
  from the environment, including:
  - Service account key from environment variables
  - Application Default Credentials from gcloud
  - Compute Engine metadata service
  - Google Cloud Run/Functions metadata service

  This is a wrapper around the Goth library that handles the complexity of credential discovery.
  """

  require Logger

  @type credentials :: %{
          client_email: String.t(),
          private_key: String.t(),
          project_id: String.t(),
          type: String.t()
        }

  @callback get_credentials() :: {:ok, credentials()} | {:error, term()}
  @callback normalize_credentials(map()) :: {:ok, map()} | {:error, term()}

  @doc """
  Gets GCP credentials from the environment using Application Default Credentials.

  This function uses Goth to discover credentials from various sources in order:
  1. GOOGLE_APPLICATION_CREDENTIALS environment variable
  2. ~/.config/gcloud/application_default_credentials.json
  3. Google Cloud metadata service

  Returns `{:ok, credentials}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> get_credentials()
      {:ok, %{client_email: "...", private_key: "...", project_id: "...", type: "service_account"}}

      iex> get_credentials()
      {:error, "No credentials available"}
  """
  @spec get_credentials() :: {:ok, map()} | {:error, term()}
  def get_credentials do
    case get_credentials_from_goth() do
      {:ok, credentials} -> {:ok, credentials}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Gets GCP credentials from the GOOGLE_APPLICATION_CREDENTIALS environment variable.

  This function is kept for backward compatibility and testing purposes.
  It's recommended to use `get_credentials/0` instead.

  Returns `{:ok, credentials}` on success or `{:error, reason}` on failure.
  """
  @spec get_service_account_from_env() :: {:ok, map()} | {:error, term()}
  def get_service_account_from_env do
    case System.get_env("GOOGLE_APPLICATION_CREDENTIALS") do
      nil ->
        {:error, "GOOGLE_APPLICATION_CREDENTIALS environment variable not set"}

      path ->
        case File.read(path) do
          {:ok, content} ->
            case Jason.decode(content) do
              {:ok, credentials} ->
                {:ok, credentials}

              {:error, reason} ->
                {:error, "Failed to parse service account JSON: #{inspect(reason)}"}
            end

          {:error, reason} ->
            {:error, "Failed to read service account file: #{inspect(reason)}"}
        end
    end
  end

  @doc """
  Gets GCP credentials from the gcloud application default credentials.

  This function is kept for backward compatibility and testing purposes.
  It's recommended to use `get_credentials/0` instead.

  Returns `{:ok, credentials}` on success or `{:error, reason}` on failure.
  """
  @spec get_application_default_credentials() :: {:ok, map()} | {:error, term()}
  def get_application_default_credentials do
    home_dir = System.user_home()
    gcloud_path = Path.join([home_dir, ".config", "gcloud", "application_default_credentials.json"])

    case File.read(gcloud_path) do
      {:ok, content} ->
        case Jason.decode(content) do
          {:ok, credentials} ->
            {:ok, credentials}

          {:error, reason} ->
            {:error, "Failed to parse application default credentials JSON: #{inspect(reason)}"}
        end

      {:error, _} ->
        {:error, "Application default credentials file not found"}
    end
  end

  @doc """
  Gets GCP credentials from the Compute Engine metadata service.

  This function is kept for backward compatibility and testing purposes.
  It's recommended to use `get_credentials/0` instead.

  Returns `{:ok, credentials}` on success or `{:error, reason}` on failure.
  """
  @spec get_compute_engine_credentials() :: {:ok, map()} | {:error, term()}
  def get_compute_engine_credentials do
    # This would require implementing the metadata service client
    # For now, we'll return an error as this is more complex
    {:error, "Compute Engine metadata service not implemented"}
  end

  @doc """
  Converts raw GCP credentials to the format expected by the PubSub client.

  Returns `{:ok, credentials}` on success or `{:error, reason}` on failure.
  """
  @spec normalize_credentials(map()) :: {:ok, map()} | {:error, term()}
  def normalize_credentials(raw_credentials) do
    case raw_credentials do
      %{"type" => "service_account"} = creds ->
        {:ok,
         %{
           type: "service_account",
           project_id: creds["project_id"],
           private_key_id: creds["private_key_id"],
           private_key: creds["private_key"],
           client_email: creds["client_email"],
           client_id: creds["client_id"],
           auth_uri: creds["auth_uri"],
           token_uri: creds["token_uri"],
           auth_provider_x509_cert_url: creds["auth_provider_x509_cert_url"],
           client_x509_cert_url: creds["client_x509_cert_url"],
           universe_domain: creds["universe_domain"] || "googleapis.com"
         }}

      %{"type" => "authorized_user"} = _creds ->
        # For authorized user credentials, we need to handle token refresh
        # This is more complex and would require implementing OAuth2 flow
        {:error, "Authorized user credentials not supported"}

      %{"type" => "metadata_service"} = creds ->
        # For metadata service, we return a simplified structure
        {:ok,
         %{
           type: "metadata_service",
           project_id: creds["project_id"],
           token: creds["token"],
           expires_at: creds["expires_at"]
         }}

      _ ->
        {:error, "Unsupported credential type"}
    end
  end

  # Private function to get credentials using Goth
  defp get_credentials_from_goth do
    # Use Goth's default credential discovery to find the source
    case resolve_goth_source() do
      {:ok, source} ->
        # Now get the actual credentials from the source
        get_credentials_from_source(source)

      {:error, reason} ->
        {:error, reason}
    end
  rescue
    error ->
      {:error, "Error getting credentials from Goth: #{inspect(error)}"}
  end

  # Resolve the Goth source using the same precedence as Goth
  defp resolve_goth_source do
    cond do
      # Check GOOGLE_APPLICATION_CREDENTIALS environment variable
      System.get_env("GOOGLE_APPLICATION_CREDENTIALS") ->
        {:ok, {:service_account, get_service_account_from_env_path()}}

      # Check GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable
      System.get_env("GOOGLE_APPLICATION_CREDENTIALS_JSON") ->
        case Jason.decode(System.get_env("GOOGLE_APPLICATION_CREDENTIALS_JSON")) do
          {:ok, credentials} -> {:ok, {:service_account, credentials}}
          {:error, reason} -> {:error, "Invalid JSON in GOOGLE_APPLICATION_CREDENTIALS_JSON: #{inspect(reason)}"}
        end

      # Check application default credentials file
      adc_file_exists?() ->
        case get_application_default_credentials() do
          {:ok, credentials} -> {:ok, {:authorized_user, credentials}}
          {:error, reason} -> {:error, reason}
        end

      # Fallback to metadata service (handled by Goth)
      true ->
        {:ok, :metadata}
    end
  end

  # Get credentials from the resolved source
  defp get_credentials_from_source({:service_account, path}) when is_binary(path) do
    case File.read(path) do
      {:ok, content} ->
        case Jason.decode(content) do
          {:ok, credentials} -> {:ok, credentials}
          {:error, reason} -> {:error, "Failed to parse service account JSON: #{inspect(reason)}"}
        end

      {:error, reason} ->
        {:error, "Failed to read service account file: #{inspect(reason)}"}
    end
  end

  defp get_credentials_from_source({:service_account, credentials}) when is_map(credentials) do
    {:ok, credentials}
  end

  defp get_credentials_from_source({:authorized_user, credentials}) do
    {:ok, credentials}
  end

  defp get_credentials_from_source(:metadata) do
    # For metadata service, we'll use Goth to get the token and then
    # create a synthetic credentials structure
    case Goth.Token.fetch(source: {:metadata, []}) do
      {:ok, token} ->
        # Create a synthetic credentials structure for metadata service
        project_id = get_project_id_from_metadata()

        {:ok,
         %{
           "type" => "metadata_service",
           "project_id" => project_id,
           "token" => token.token,
           "expires_at" => token.expires
         }}

      {:error, _reason} ->
        {:error, "No credentials available"}
    end
  end

  # Helper functions
  defp get_service_account_from_env_path do
    System.get_env("GOOGLE_APPLICATION_CREDENTIALS")
  end

  defp adc_file_exists? do
    home_dir = System.user_home()
    gcloud_path = Path.join([home_dir, ".config", "gcloud", "application_default_credentials.json"])
    File.exists?(gcloud_path)
  end

  defp get_project_id_from_metadata do
    # This would normally make an HTTP request to the metadata service
    # For now, we'll return a placeholder
    "unknown-project"
  end
end
