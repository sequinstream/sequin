defmodule Sequin.Gcp.ApplicationDefaultCredentials do
  @moduledoc """
  Handles GCP Application Default Credentials (ADC) resolution.

  This module provides functionality to automatically discover and use GCP credentials
  from the environment, including:
  - Service account key from GOOGLE_APPLICATION_CREDENTIALS environment variable
  - Service account key from GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable  
  - Application Default Credentials from gcloud (~/.config/gcloud/application_default_credentials.json)
  - Compute Engine metadata service (future implementation)

  Note: While Goth is available in the project, this module implements manual credential
  discovery to have direct access to the service account credentials needed by the
  PubSub client.
  """

  require Logger

  @type credentials :: %{
          client_email: String.t(),
          private_key: String.t(),
          project_id: String.t(),
          private_key_id: String.t(),
          client_id: String.t(),
          auth_uri: String.t(),
          token_uri: String.t(),
          auth_provider_x509_cert_url: String.t(),
          client_x509_cert_url: String.t(),
          universe_domain: String.t(),
          type: String.t()
        }

  @callback get_credentials() :: {:ok, credentials()} | {:error, term()}

  @doc """
  Gets GCP credentials from the environment using Application Default Credentials.

  This function discovers credentials from various sources in order:
  1. GOOGLE_APPLICATION_CREDENTIALS environment variable (path to JSON file)
  2. GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable (JSON string)
  3. ~/.config/gcloud/application_default_credentials.json
  4. Google Cloud metadata service (not yet implemented)

  Returns `{:ok, credentials}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> get_credentials()
      {:ok, %{client_email: "...", private_key: "...", project_id: "...", type: "service_account"}}

      iex> get_credentials()
      {:error, "No credentials available"}
  """
  @spec get_credentials() :: {:ok, map()} | {:error, term()}
  def get_credentials do
    with {:error, _} <- get_service_account_from_env(),
         {:error, _} <- get_service_account_from_env_json(),
         {:error, _} <- get_application_default_credentials(),
         {:error, _} <- get_compute_engine_credentials() do
      {:error, "No credentials available"}
    end
  end

  @doc """
  Gets service account credentials from GOOGLE_APPLICATION_CREDENTIALS environment variable.
  """
  @spec get_service_account_from_env() :: {:ok, map()} | {:error, String.t()}
  def get_service_account_from_env do
    case System.get_env("GOOGLE_APPLICATION_CREDENTIALS") do
      nil ->
        {:error, "GOOGLE_APPLICATION_CREDENTIALS environment variable not set"}

      path ->
        case File.read(path) do
          {:ok, content} ->
            case Jason.decode(content) do
              {:ok, credentials} -> {:ok, credentials}
              {:error, error} -> {:error, "Failed to parse service account JSON: #{inspect(error)}"}
            end

          {:error, reason} ->
            {:error, "Failed to read service account file: #{inspect(reason)}"}
        end
    end
  end

  @doc """
  Gets service account credentials from GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable.
  """
  @spec get_service_account_from_env_json() :: {:ok, map()} | {:error, String.t()}
  def get_service_account_from_env_json do
    case System.get_env("GOOGLE_APPLICATION_CREDENTIALS_JSON") do
      nil ->
        {:error, "GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable not set"}

      json_string ->
        case Jason.decode(json_string) do
          {:ok, credentials} -> {:ok, credentials}
          {:error, error} -> {:error, "Failed to parse service account JSON: #{inspect(error)}"}
        end
    end
  end

  @doc """
  Gets application default credentials from gcloud configuration.
  """
  @spec get_application_default_credentials() :: {:ok, map()} | {:error, String.t()}
  def get_application_default_credentials do
    home = System.get_env("HOME")

    if home do
      path = Path.join([home, ".config", "gcloud", "application_default_credentials.json"])

      case File.read(path) do
        {:ok, content} ->
          case Jason.decode(content) do
            {:ok, credentials} -> {:ok, credentials}
            {:error, error} -> {:error, "Failed to parse ADC JSON: #{inspect(error)}"}
          end

        {:error, _} ->
          {:error, "Application default credentials file not found"}
      end
    else
      {:error, "HOME environment variable not set"}
    end
  end

  @doc """
  Gets credentials from Compute Engine metadata service.
  Not yet implemented.
  """
  @spec get_compute_engine_credentials() :: {:ok, map()} | {:error, String.t()}
  def get_compute_engine_credentials do
    # TODO: Implement metadata service access
    {:error, "Compute Engine metadata service not implemented"}
  end

  @doc """
  Normalizes raw credentials into a consistent format.
  """
  @spec normalize_credentials(map()) :: {:ok, map()} | {:error, String.t()}
  def normalize_credentials(raw_credentials) do
    case Map.get(raw_credentials, "type") do
      "service_account" ->
        normalized = %{
          type: "service_account",
          project_id: Map.get(raw_credentials, "project_id"),
          private_key_id: Map.get(raw_credentials, "private_key_id"),
          private_key: Map.get(raw_credentials, "private_key"),
          client_email: Map.get(raw_credentials, "client_email"),
          client_id: Map.get(raw_credentials, "client_id"),
          auth_uri: Map.get(raw_credentials, "auth_uri"),
          token_uri: Map.get(raw_credentials, "token_uri"),
          auth_provider_x509_cert_url: Map.get(raw_credentials, "auth_provider_x509_cert_url"),
          client_x509_cert_url: Map.get(raw_credentials, "client_x509_cert_url"),
          universe_domain: Map.get(raw_credentials, "universe_domain", "googleapis.com")
        }

        {:ok, normalized}

      "authorized_user" ->
        {:error, "Authorized user credentials not supported"}

      _ ->
        {:error, "Unsupported credential type"}
    end
  end
end
