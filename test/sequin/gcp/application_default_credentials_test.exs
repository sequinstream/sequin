defmodule Sequin.Gcp.ApplicationDefaultCredentialsTest do
  use ExUnit.Case, async: true

  alias Sequin.Gcp.ApplicationDefaultCredentials

  describe "normalize_credentials/1" do
    test "normalizes service account credentials" do
      raw_credentials = %{
        "type" => "service_account",
        "project_id" => "test-project",
        "private_key_id" => "key-id",
        "private_key" => "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----\n",
        "client_email" => "test@test-project.iam.gserviceaccount.com",
        "client_id" => "123456789",
        "auth_uri" => "https://accounts.google.com/o/oauth2/auth",
        "token_uri" => "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url" => "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url" =>
          "https://www.googleapis.com/robot/v1/metadata/x509/test%40test-project.iam.gserviceaccount.com"
      }

      assert {:ok, normalized} = ApplicationDefaultCredentials.normalize_credentials(raw_credentials)

      assert normalized.type == "service_account"
      assert normalized.project_id == "test-project"
      assert normalized.private_key_id == "key-id"
      assert normalized.private_key == "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----\n"
      assert normalized.client_email == "test@test-project.iam.gserviceaccount.com"
      assert normalized.client_id == "123456789"
      assert normalized.auth_uri == "https://accounts.google.com/o/oauth2/auth"
      assert normalized.token_uri == "https://oauth2.googleapis.com/token"
      assert normalized.auth_provider_x509_cert_url == "https://www.googleapis.com/oauth2/v1/certs"

      assert normalized.client_x509_cert_url ==
               "https://www.googleapis.com/robot/v1/metadata/x509/test%40test-project.iam.gserviceaccount.com"

      assert normalized.universe_domain == "googleapis.com"
    end

    test "normalizes service account credentials with universe domain" do
      raw_credentials = %{
        "type" => "service_account",
        "project_id" => "test-project",
        "private_key_id" => "key-id",
        "private_key" => "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----\n",
        "client_email" => "test@test-project.iam.gserviceaccount.com",
        "client_id" => "123456789",
        "auth_uri" => "https://accounts.google.com/o/oauth2/auth",
        "token_uri" => "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url" => "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url" =>
          "https://www.googleapis.com/robot/v1/metadata/x509/test%40test-project.iam.gserviceaccount.com",
        "universe_domain" => "custom.googleapis.com"
      }

      assert {:ok, normalized} = ApplicationDefaultCredentials.normalize_credentials(raw_credentials)
      assert normalized.universe_domain == "custom.googleapis.com"
    end

    test "returns error for authorized user credentials" do
      raw_credentials = %{
        "type" => "authorized_user",
        "client_id" => "123456789",
        "client_secret" => "secret"
      }

      assert {:error, "Authorized user credentials not supported"} =
               ApplicationDefaultCredentials.normalize_credentials(raw_credentials)
    end

    test "returns error for unsupported credential type" do
      raw_credentials = %{"type" => "unsupported"}

      assert {:error, "Unsupported credential type"} =
               ApplicationDefaultCredentials.normalize_credentials(raw_credentials)
    end
  end

  describe "get_service_account_from_env/0" do
    test "returns credentials from GOOGLE_APPLICATION_CREDENTIALS" do
      credentials = %{
        "type" => "service_account",
        "project_id" => "test-project",
        "private_key_id" => "key-id",
        "private_key" => "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----\n",
        "client_email" => "test@test-project.iam.gserviceaccount.com",
        "client_id" => "123456789"
      }

      temp_file = Path.join(System.tmp_dir!(), "service_account_#{System.unique_integer()}.json")
      File.write!(temp_file, Jason.encode!(credentials))

      System.put_env("GOOGLE_APPLICATION_CREDENTIALS", temp_file)

      assert {:ok, ^credentials} = ApplicationDefaultCredentials.get_service_account_from_env()

      # Clean up
      File.rm!(temp_file)
      System.delete_env("GOOGLE_APPLICATION_CREDENTIALS")
    end

    test "returns error when GOOGLE_APPLICATION_CREDENTIALS is not set" do
      System.delete_env("GOOGLE_APPLICATION_CREDENTIALS")

      assert {:error, "GOOGLE_APPLICATION_CREDENTIALS environment variable not set"} =
               ApplicationDefaultCredentials.get_service_account_from_env()
    end

    test "returns error when file does not exist" do
      System.put_env("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent/path.json")

      assert {:error, error} = ApplicationDefaultCredentials.get_service_account_from_env()
      assert String.starts_with?(error, "Failed to read service account file:")

      # Clean up
      System.delete_env("GOOGLE_APPLICATION_CREDENTIALS")
    end

    test "returns error when file contains invalid JSON" do
      temp_file = Path.join(System.tmp_dir!(), "invalid_json_#{System.unique_integer()}.json")
      File.write!(temp_file, "invalid json")

      System.put_env("GOOGLE_APPLICATION_CREDENTIALS", temp_file)

      assert {:error, error} = ApplicationDefaultCredentials.get_service_account_from_env()
      assert String.starts_with?(error, "Failed to parse service account JSON:")

      # Clean up
      File.rm!(temp_file)
      System.delete_env("GOOGLE_APPLICATION_CREDENTIALS")
    end
  end

  describe "get_application_default_credentials/0" do
    test "returns error when application default credentials file does not exist" do
      assert {:error, "Application default credentials file not found"} =
               ApplicationDefaultCredentials.get_application_default_credentials()
    end
  end

  describe "get_compute_engine_credentials/0" do
    test "returns error as not implemented" do
      assert {:error, "Compute Engine metadata service not implemented"} =
               ApplicationDefaultCredentials.get_compute_engine_credentials()
    end
  end

  describe "get_credentials/0" do
    test "returns credentials from environment when available" do
      credentials = %{
        "type" => "service_account",
        "project_id" => "test-project",
        "private_key_id" => "key-id",
        "private_key" => "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----\n",
        "client_email" => "test@test-project.iam.gserviceaccount.com",
        "client_id" => "123456789"
      }

      temp_file = Path.join(System.tmp_dir!(), "service_account_#{System.unique_integer()}.json")
      File.write!(temp_file, Jason.encode!(credentials))

      System.put_env("GOOGLE_APPLICATION_CREDENTIALS", temp_file)

      assert {:ok, ^credentials} = ApplicationDefaultCredentials.get_credentials()

      # Clean up
      File.rm!(temp_file)
      System.delete_env("GOOGLE_APPLICATION_CREDENTIALS")
    end

    test "returns error when no credentials are available" do
      System.delete_env("GOOGLE_APPLICATION_CREDENTIALS")

      assert {:error, "No credentials available"} = ApplicationDefaultCredentials.get_credentials()
    end
  end
end
