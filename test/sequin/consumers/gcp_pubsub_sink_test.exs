defmodule Sequin.Consumers.GcpPubsubSinkTest do
  use ExUnit.Case, async: true

  import Mox

  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Gcp.ApplicationDefaultCredentialsMock

  setup :verify_on_exit!

  setup do
    # Configure application to use the mock
    Application.put_env(:sequin, :gcp_credentials_module, ApplicationDefaultCredentialsMock)

    on_exit(fn ->
      Application.delete_env(:sequin, :gcp_credentials_module)
    end)

    :ok
  end

  describe "changeset/2" do
    test "validates explicit credentials when use_application_default_credentials is false" do
      changeset =
        GcpPubsubSink.changeset(%GcpPubsubSink{}, %{
          project_id: "test-project",
          topic_id: "test-topic",
          use_emulator: false,
          use_application_default_credentials: false,
          routing_mode: :static
        })

      refute changeset.valid?
      assert changeset.errors[:credentials] == {"can't be blank", [validation: :required]}
    end

    test "only requires project_id when use_application_default_credentials is true" do
      changeset =
        GcpPubsubSink.changeset(%GcpPubsubSink{}, %{
          project_id: "test-project",
          topic_id: "test-topic",
          use_emulator: false,
          use_application_default_credentials: true,
          routing_mode: :static
        })

      assert changeset.valid?
      refute changeset.errors[:credentials]
    end

    test "validates project_id format" do
      changeset =
        GcpPubsubSink.changeset(%GcpPubsubSink{}, %{
          project_id: "invalid-project-id-too-long-and-has-capital-letters",
          topic_id: "test-topic",
          use_emulator: false,
          use_application_default_credentials: true,
          routing_mode: :static
        })

      refute changeset.valid?
      assert changeset.errors[:project_id]
    end

    test "validates topic_id format" do
      changeset =
        GcpPubsubSink.changeset(%GcpPubsubSink{}, %{
          project_id: "test-project",
          topic_id: "invalid topic id with spaces",
          use_emulator: false,
          use_application_default_credentials: true,
          routing_mode: :static
        })

      refute changeset.valid?
      assert changeset.errors[:topic_id]
    end

    test "sets topic_id to nil when routing_mode is dynamic" do
      # Store original value and set self_hosted to true to allow application default credentials
      original_self_hosted = Application.get_env(:sequin, :self_hosted)
      Application.put_env(:sequin, :self_hosted, true)

      changeset =
        GcpPubsubSink.changeset(%GcpPubsubSink{}, %{
          project_id: "test-project",
          topic_id: "test-topic",
          use_emulator: false,
          use_application_default_credentials: true,
          routing_mode: :dynamic
        })

      assert changeset.valid?
      assert Ecto.Changeset.get_field(changeset, :topic_id) == nil

      # Restore original setting
      Application.put_env(:sequin, :self_hosted, original_self_hosted)
    end

    test "validates topic_id is required when routing_mode is static" do
      changeset =
        GcpPubsubSink.changeset(%GcpPubsubSink{}, %{
          project_id: "test-project",
          use_emulator: false,
          use_application_default_credentials: true,
          routing_mode: :static
        })

      refute changeset.valid?
      assert changeset.errors[:topic_id] == {"can't be blank", [validation: :required]}
    end

    test "validates emulator_base_url when use_emulator is true" do
      changeset =
        GcpPubsubSink.changeset(%GcpPubsubSink{}, %{
          project_id: "test-project",
          use_emulator: true,
          routing_mode: :static
        })

      refute changeset.valid?
      assert changeset.errors[:emulator_base_url] == {"can't be blank", [validation: :required]}
    end

    test "validates emulator_base_url format" do
      changeset =
        GcpPubsubSink.changeset(%GcpPubsubSink{}, %{
          project_id: "test-project",
          use_emulator: true,
          emulator_base_url: "invalid-url",
          routing_mode: :static
        })

      refute changeset.valid?
      assert changeset.errors[:emulator_base_url]
    end

    test "generates connection_id when not provided" do
      changeset =
        GcpPubsubSink.changeset(%GcpPubsubSink{}, %{
          project_id: "test-project",
          topic_id: "test-topic",
          use_emulator: false,
          use_application_default_credentials: true,
          routing_mode: :static
        })

      assert changeset.valid?
      assert Ecto.Changeset.get_field(changeset, :connection_id) != nil
    end

    test "preserves existing connection_id" do
      existing_id = Ecto.UUID.generate()

      changeset =
        GcpPubsubSink.changeset(%GcpPubsubSink{connection_id: existing_id}, %{
          project_id: "test-project",
          topic_id: "test-topic",
          use_emulator: false,
          use_application_default_credentials: true,
          routing_mode: :static
        })

      assert changeset.valid?
      assert Ecto.Changeset.get_field(changeset, :connection_id) == existing_id
    end
  end

  describe "pubsub_client/1" do
    test "creates client with explicit credentials when use_application_default_credentials is false" do
      credentials = %{
        "type" => "service_account",
        "project_id" => "test-project",
        "private_key_id" => "key-id",
        "private_key" => "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----\n",
        "client_email" => "test@test-project.iam.gserviceaccount.com",
        "client_id" => "123456789"
      }

      sink = %GcpPubsubSink{
        project_id: "test-project",
        credentials: credentials,
        use_emulator: false,
        use_application_default_credentials: false
      }

      client = GcpPubsubSink.pubsub_client(sink)

      assert client.project_id == "test-project"
      assert client.credentials.type == "service_account"
      assert client.credentials.project_id == "test-project"
      assert client.credentials.client_id == "123456789"
      assert client.use_emulator == false
    end

    test "creates client with application default credentials when use_application_default_credentials is true" do
      # Mock the application default credentials
      mock_credentials = %{
        "type" => "service_account",
        "project_id" => "test-project",
        "private_key_id" => "key-id",
        "private_key" => "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----\n",
        "client_email" => "test@test-project.iam.gserviceaccount.com",
        "client_id" => "123456789"
      }

      normalized_credentials = %{
        type: "service_account",
        project_id: "test-project",
        private_key_id: "key-id",
        private_key: "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----\n",
        client_email: "test@test-project.iam.gserviceaccount.com",
        client_id: "123456789",
        auth_uri: "https://accounts.google.com/o/oauth2/auth",
        token_uri: "https://oauth2.googleapis.com/token",
        auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs",
        client_x509_cert_url:
          "https://www.googleapis.com/robot/v1/metadata/x509/test%40test-project.iam.gserviceaccount.com",
        universe_domain: "googleapis.com"
      }

      expect(ApplicationDefaultCredentialsMock, :get_credentials, fn ->
        {:ok, mock_credentials}
      end)

      expect(ApplicationDefaultCredentialsMock, :normalize_credentials, fn ^mock_credentials ->
        {:ok, normalized_credentials}
      end)

      sink = %GcpPubsubSink{
        project_id: "test-project",
        use_emulator: false,
        use_application_default_credentials: true
      }

      client = GcpPubsubSink.pubsub_client(sink)

      assert client.project_id == "test-project"
      assert client.credentials.type == "service_account"
      assert client.credentials.project_id == "test-project"
      assert client.credentials.client_id == "123456789"
      assert client.use_emulator == false
    end

    test "creates emulator client when use_emulator is true" do
      sink = %GcpPubsubSink{
        project_id: "test-project",
        use_emulator: true,
        emulator_base_url: "http://localhost:8085"
      }

      client = GcpPubsubSink.pubsub_client(sink)

      assert client.project_id == "test-project"
      assert client.use_emulator == true
      assert client.req_opts[:base_url] == "http://localhost:8085"
    end

    test "raises error when application default credentials are unavailable" do
      expect(ApplicationDefaultCredentialsMock, :get_credentials, fn ->
        {:error, "No credentials available"}
      end)

      sink = %GcpPubsubSink{
        project_id: "test-project",
        use_emulator: false,
        use_application_default_credentials: true
      }

      assert_raise RuntimeError, "Failed to get application default credentials: \"No credentials available\"", fn ->
        GcpPubsubSink.pubsub_client(sink)
      end
    end

    test "raises error when credentials normalization fails" do
      mock_credentials = %{
        "type" => "authorized_user",
        "client_id" => "123456789",
        "client_secret" => "secret"
      }

      expect(ApplicationDefaultCredentialsMock, :get_credentials, fn ->
        {:ok, mock_credentials}
      end)

      expect(ApplicationDefaultCredentialsMock, :normalize_credentials, fn ^mock_credentials ->
        {:error, "Authorized user credentials not supported"}
      end)

      sink = %GcpPubsubSink{
        project_id: "test-project",
        use_emulator: false,
        use_application_default_credentials: true
      }

      assert_raise RuntimeError,
                   "Failed to get application default credentials: \"Authorized user credentials not supported\"",
                   fn ->
                     GcpPubsubSink.pubsub_client(sink)
                   end
    end
  end

  describe "topic_path/1" do
    test "generates correct topic path" do
      sink = %GcpPubsubSink{
        project_id: "test-project",
        topic_id: "test-topic"
      }

      assert GcpPubsubSink.topic_path(sink) == "projects/test-project/topics/test-topic"
    end
  end

  describe "changeset/2 cloud mode restrictions" do
    test "validates that use_application_default_credentials is false when not self_hosted" do
      # Store original value and set to cloud mode
      original_self_hosted = Application.get_env(:sequin, :self_hosted)
      Application.put_env(:sequin, :self_hosted, false)

      params = %{
        project_id: "test-project",
        use_emulator: false,
        use_application_default_credentials: true,
        routing_mode: :dynamic
      }

      changeset = GcpPubsubSink.changeset(%GcpPubsubSink{}, params)

      refute changeset.valid?

      assert changeset.errors[:use_application_default_credentials] ==
               {"Application Default Credentials are not supported in Sequin Cloud. Please use explicit credentials instead.",
                []}

      # Restore the original setting
      Application.put_env(:sequin, :self_hosted, original_self_hosted)
    end

    test "allows use_application_default_credentials when self_hosted" do
      # Store original value and set to self-hosted mode
      original_self_hosted = Application.get_env(:sequin, :self_hosted)
      Application.put_env(:sequin, :self_hosted, true)

      params = %{
        project_id: "test-project",
        use_emulator: false,
        use_application_default_credentials: true,
        routing_mode: :dynamic
      }

      changeset = GcpPubsubSink.changeset(%GcpPubsubSink{}, params)

      assert changeset.valid?
      refute changeset.errors[:use_application_default_credentials]

      # Restore the original setting
      Application.put_env(:sequin, :self_hosted, original_self_hosted)
    end

    test "allows use_application_default_credentials=false in cloud mode" do
      # Store original value and set to cloud mode
      original_self_hosted = Application.get_env(:sequin, :self_hosted)
      Application.put_env(:sequin, :self_hosted, false)

      params = %{
        project_id: "test-project",
        use_emulator: false,
        use_application_default_credentials: false,
        routing_mode: :dynamic,
        credentials: %{
          type: "service_account",
          private_key: "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----\n",
          client_email: "test@test-project.iam.gserviceaccount.com",
          project_id: "test-project",
          private_key_id: "key-id",
          client_id: "123456789"
        }
      }

      changeset = GcpPubsubSink.changeset(%GcpPubsubSink{}, params)

      assert changeset.valid?
      refute changeset.errors[:use_application_default_credentials]

      # Restore the original setting
      Application.put_env(:sequin, :self_hosted, original_self_hosted)
    end
  end
end
