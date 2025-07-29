defmodule Sequin.Consumers.GcpPubsubSinkTest do
  use Sequin.Case, async: true

  alias Sequin.Consumers.GcpPubsubSink
  alias Sequin.Test.GcpTestHelpers

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

    @tag self_hosted: true
    test "sets topic_id to nil when routing_mode is dynamic" do
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
      GcpTestHelpers.setup_application_default_credentials_stub(project_id: "test-project")

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
      GcpTestHelpers.setup_failed_application_default_credentials_stub()

      sink = %GcpPubsubSink{
        project_id: "test-project",
        use_emulator: false,
        use_application_default_credentials: true
      }

      assert_raise RuntimeError, ~r/Failed to create PubSub client/, fn ->
        GcpPubsubSink.pubsub_client(sink)
      end
    end

    test "raises error when credentials normalization fails" do
      GcpTestHelpers.setup_authorized_user_credentials_stub()

      sink = %GcpPubsubSink{
        project_id: "test-project",
        use_emulator: false,
        use_application_default_credentials: true
      }

      assert_raise RuntimeError,
                   ~r/Failed to create PubSub client/,
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
    @tag self_hosted: false
    test "validates that use_application_default_credentials is false when not self_hosted" do
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
    end

    @tag self_hosted: true
    test "allows use_application_default_credentials when self_hosted" do
      params = %{
        project_id: "test-project",
        use_emulator: false,
        use_application_default_credentials: true,
        routing_mode: :dynamic
      }

      changeset = GcpPubsubSink.changeset(%GcpPubsubSink{}, params)

      assert changeset.valid?
      refute changeset.errors[:use_application_default_credentials]
    end

    @tag self_hosted: false
    test "allows use_application_default_credentials=false in cloud mode" do
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
    end
  end
end
