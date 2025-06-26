defmodule Sequin.Consumers.KafkaSinkTest do
  use ExUnit.Case, async: true

  alias Sequin.Consumers.KafkaSink

  describe "kafka_url/2" do
    test "generates basic kafka URL without authentication" do
      sink = %KafkaSink{
        hosts: "localhost:9092",
        topic: "test-topic"
      }

      assert KafkaSink.kafka_url(sink) == "kafka://localhost:9092"
    end

    test "generates URL with username only" do
      sink = %KafkaSink{
        hosts: "localhost:9092",
        username: "user1",
        topic: "test-topic"
      }

      assert KafkaSink.kafka_url(sink) == "kafka://user1@localhost:9092"
    end

    test "generates URL with username and password" do
      sink = %KafkaSink{
        hosts: "localhost:9092",
        username: "user1",
        password: "secret",
        topic: "test-topic"
      }

      assert KafkaSink.kafka_url(sink) == "kafka://user1:******@localhost:9092"
      assert KafkaSink.kafka_url(sink, obscure_password: false) == "kafka://user1:secret@localhost:9092"
    end

    test "generates URL with TLS enabled" do
      sink = %KafkaSink{
        hosts: "localhost:9092",
        tls: true,
        topic: "test-topic"
      }

      assert KafkaSink.kafka_url(sink) == "kafka+ssl://localhost:9092"
    end

    test "handles multiple hosts" do
      sink = %KafkaSink{
        hosts: "localhost:9092,remote:9093,other:9094",
        topic: "test-topic"
      }

      assert KafkaSink.kafka_url(sink) == "kafka://localhost:9092,remote:9093,other:9094"
    end

    test "generates URL with SASL PLAIN authentication" do
      sink = %KafkaSink{
        hosts: "localhost:9092",
        username: "user1",
        password: "secret",
        topic: "test-topic",
        sasl_mechanism: :plain,
        tls: false
      }

      assert KafkaSink.kafka_url(sink) == "kafka://user1:******@localhost:9092"
      assert KafkaSink.kafka_url(sink, obscure_password: false) == "kafka://user1:secret@localhost:9092"
    end

    test "generates URL with SASL SCRAM authentication" do
      sink = %KafkaSink{
        hosts: "localhost:9092",
        username: "user1",
        password: "secret",
        topic: "test-topic",
        sasl_mechanism: :scram_sha_256,
        tls: false
      }

      assert KafkaSink.kafka_url(sink) == "kafka://user1:******@localhost:9092"
    end

    test "generates URL with TLS and SASL" do
      sink = %KafkaSink{
        hosts: "localhost:9092",
        username: "user1",
        password: "secret",
        topic: "test-topic",
        sasl_mechanism: :plain,
        tls: true
      }

      assert KafkaSink.kafka_url(sink) == "kafka+ssl://user1:******@localhost:9092"
    end
  end

  describe "changeset/2" do
    test "validates required fields" do
      changeset = KafkaSink.changeset(%KafkaSink{}, %{})
      refute changeset.valid?
      assert "can't be blank" in errors_on(changeset).hosts
      assert "is required" in errors_on(changeset).routing_mode
    end

    test "validates SASL credentials when mechanism is set" do
      for mechanism <- [:plain, :scram_sha_256, :scram_sha_512] do
        # Test missing credentials
        changeset =
          KafkaSink.changeset(%KafkaSink{}, %{
            hosts: "localhost:9092",
            topic: "test-topic",
            tls: false,
            sasl_mechanism: mechanism,
            routing_mode: :static
          })

        refute changeset.valid?
        assert "is required when SASL Mechanism is #{mechanism}" in errors_on(changeset).username
        assert "is required when SASL Mechanism is #{mechanism}" in errors_on(changeset).password

        # Test with valid credentials
        changeset =
          KafkaSink.changeset(%KafkaSink{}, %{
            hosts: "localhost:9092",
            topic: "test-topic",
            tls: false,
            sasl_mechanism: mechanism,
            username: "user1",
            password: "secret",
            routing_mode: :static
          })

        assert changeset.valid?
      end
    end

    test "allows missing credentials when SASL is not configured" do
      changeset =
        KafkaSink.changeset(%KafkaSink{}, %{
          hosts: "localhost:9092",
          topic: "test-topic",
          tls: false,
          routing_mode: :static
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
          KafkaSink.changeset(%KafkaSink{}, %{
            hosts: hosts,
            topic: "test-topic",
            routing_mode: :static
          })

        refute changeset.valid?, "Expected #{hosts} to be invalid"

        assert "must be a comma-separated list of host:port pairs with valid ports (1-65535)" in errors_on(changeset).hosts
      end

      for hosts <- valid_hosts do
        changeset =
          KafkaSink.changeset(%KafkaSink{}, %{
            hosts: hosts,
            topic: "test-topic",
            routing_mode: :static
          })

        assert changeset.valid?, "Expected #{hosts} to be valid"
      end
    end

    test "validates topic length" do
      params = %{
        hosts: "localhost:9092",
        topic: String.duplicate("a", 256)
      }

      changeset = KafkaSink.changeset(%KafkaSink{}, params)
      refute changeset.valid?
      assert "should be at most 255 character(s)" in errors_on(changeset).topic
    end

    test "sets topic to blank when routing_mode is dynamic" do
      changeset =
        KafkaSink.changeset(%KafkaSink{}, %{
          hosts: "localhost:9092",
          topic: "test-topic",
          routing_mode: :dynamic
        })

      refute :topic in changeset.changes
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
