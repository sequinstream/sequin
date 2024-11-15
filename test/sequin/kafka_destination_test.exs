defmodule Sequin.Consumers.KafkaDestinationTest do
  use ExUnit.Case, async: true

  alias Sequin.Consumers.KafkaDestination

  describe "kafka_url/2" do
    test "generates basic kafka URL without authentication" do
      destination = %KafkaDestination{
        hosts: "localhost:9092",
        topic: "test-topic"
      }

      assert KafkaDestination.kafka_url(destination) == "kafka://localhost:9092"
    end

    test "generates URL with username only" do
      destination = %KafkaDestination{
        hosts: "localhost:9092",
        username: "user1",
        topic: "test-topic"
      }

      assert KafkaDestination.kafka_url(destination) == "kafka://user1@localhost:9092"
    end

    test "generates URL with username and password" do
      destination = %KafkaDestination{
        hosts: "localhost:9092",
        username: "user1",
        password: "secret",
        topic: "test-topic"
      }

      assert KafkaDestination.kafka_url(destination) == "kafka://user1:******@localhost:9092"
      assert KafkaDestination.kafka_url(destination, obscure_password: false) == "kafka://user1:secret@localhost:9092"
    end

    test "generates URL with TLS enabled" do
      destination = %KafkaDestination{
        hosts: "localhost:9092",
        tls: true,
        topic: "test-topic"
      }

      assert KafkaDestination.kafka_url(destination) == "kafka+ssl://localhost:9092"
    end

    test "handles multiple hosts" do
      destination = %KafkaDestination{
        hosts: "localhost:9092,remote:9093,other:9094",
        topic: "test-topic"
      }

      assert KafkaDestination.kafka_url(destination) == "kafka://localhost:9092,remote:9093,other:9094"
    end

    test "generates URL with SASL PLAIN authentication" do
      destination = %KafkaDestination{
        hosts: "localhost:9092",
        username: "user1",
        password: "secret",
        topic: "test-topic",
        sasl_mechanism: :plain,
        tls: false
      }

      assert KafkaDestination.kafka_url(destination) == "kafka://user1:******@localhost:9092"
      assert KafkaDestination.kafka_url(destination, obscure_password: false) == "kafka://user1:secret@localhost:9092"
    end

    test "generates URL with SASL SCRAM authentication" do
      destination = %KafkaDestination{
        hosts: "localhost:9092",
        username: "user1",
        password: "secret",
        topic: "test-topic",
        sasl_mechanism: :scram_sha_256,
        tls: false
      }

      assert KafkaDestination.kafka_url(destination) == "kafka://user1:******@localhost:9092"
    end

    test "generates URL with TLS and SASL" do
      destination = %KafkaDestination{
        hosts: "localhost:9092",
        username: "user1",
        password: "secret",
        topic: "test-topic",
        sasl_mechanism: :plain,
        tls: true
      }

      assert KafkaDestination.kafka_url(destination) == "kafka+ssl://user1:******@localhost:9092"
    end
  end

  describe "changeset/2" do
    test "validates required fields" do
      changeset = KafkaDestination.changeset(%KafkaDestination{}, %{})
      refute changeset.valid?
      assert "can't be blank" in errors_on(changeset).hosts
      assert "can't be blank" in errors_on(changeset).topic
    end

    test "validates SASL credentials when mechanism is set" do
      for mechanism <- [:plain, :scram_sha_256, :scram_sha_512] do
        # Test missing credentials
        changeset =
          KafkaDestination.changeset(%KafkaDestination{}, %{
            hosts: "localhost:9092",
            topic: "test-topic",
            tls: false,
            sasl_mechanism: mechanism
          })

        refute changeset.valid?
        assert "is required when SASL is enabled" in errors_on(changeset).username
        assert "is required when SASL is enabled" in errors_on(changeset).password

        # Test with valid credentials
        changeset =
          KafkaDestination.changeset(%KafkaDestination{}, %{
            hosts: "localhost:9092",
            topic: "test-topic",
            tls: false,
            sasl_mechanism: mechanism,
            username: "user1",
            password: "secret"
          })

        assert changeset.valid?
      end
    end

    test "allows missing credentials when SASL is not configured" do
      changeset =
        KafkaDestination.changeset(%KafkaDestination{}, %{
          hosts: "localhost:9092",
          topic: "test-topic",
          tls: false
        })

      assert changeset.valid?
    end

    test "validates hosts format" do
      invalid_hosts = [
        # missing port
        "localhost",
        # invalid port
        "localhost:abc",
        # port too low
        "localhost:0",
        # port too high
        "localhost:65536",
        # second host missing port
        "host1:9092,host2",
        # invalid second port
        "host1:9092,host2:invalid"
      ]

      valid_hosts = [
        "localhost:9092",
        "host1:9092,host2:9093",
        "host1:1,host2:65535"
      ]

      for hosts <- invalid_hosts do
        changeset =
          KafkaDestination.changeset(%KafkaDestination{}, %{
            hosts: hosts,
            topic: "test-topic"
          })

        refute changeset.valid?, "Expected #{hosts} to be invalid"

        assert "must be a comma-separated list of host:port pairs with valid ports (1-65535)" in errors_on(changeset).hosts
      end

      for hosts <- valid_hosts do
        changeset =
          KafkaDestination.changeset(%KafkaDestination{}, %{
            hosts: hosts,
            topic: "test-topic"
          })

        assert changeset.valid?, "Expected #{hosts} to be valid"
      end
    end

    test "validates topic length" do
      params = %{
        hosts: "localhost:9092",
        topic: String.duplicate("a", 256)
      }

      changeset = KafkaDestination.changeset(%KafkaDestination{}, params)
      refute changeset.valid?
      assert "should be at most 255 character(s)" in errors_on(changeset).topic
    end
  end

  # Helper function to extract error messages
  defp errors_on(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
      Regex.replace(~r"%{(\w+)}", msg, fn _, key ->
        opts |> Keyword.get(String.to_existing_atom(key), key) |> to_string()
      end)
    end)
  end
end
