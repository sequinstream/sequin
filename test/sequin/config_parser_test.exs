defmodule Sequin.ConfigParserTest do
  use Sequin.Case, async: true

  alias Sequin.ConfigParser

  describe "redis_config/1" do
    test "returns basic config when only REDIS_URL is set" do
      env = valid_redis_config()

      assert ConfigParser.redis_config(env)[:url] == "redis://localhost:6379"
      assert ConfigParser.redis_config(env)[:socket_options] == []
      refute ConfigParser.redis_config(env)[:tls]
    end

    test "raises error when REDIS_URL is not set" do
      assert_raise ArgumentError, ~r/REDIS_URL is not set/, fn ->
        ConfigParser.redis_config(%{})
      end
    end

    test "configures SSL for valid SSL settings" do
      for ssl_value <- ["true", "1", "verify-none"], key <- ["REDIS_SSL", "REDIS_TLS"] do
        env = valid_redis_config(%{key => ssl_value})
        config = ConfigParser.redis_config(env)

        assert config[:url] == "redis://localhost:6379"
        assert config[:tls] == [verify: :verify_none]
      end
    end

    test "does not configure SSL for disabled SSL settings" do
      for ssl_value <- ["false", "0"], key <- ["REDIS_SSL", "REDIS_TLS"] do
        env = valid_redis_config(%{key => ssl_value})
        config = ConfigParser.redis_config(env)

        assert config[:url] == "redis://localhost:6379"
        refute config[:tls]
      end
    end

    test "raises error for invalid REDIS_SSL value" do
      env = valid_redis_config(%{"REDIS_SSL" => "invalid"})

      assert_raise ArgumentError, ~r/REDIS_TLS must be true, 1, verify-none, false, or 0/, fn ->
        ConfigParser.redis_config(env)
      end
    end

    test "raises error for invalid REDIS_TLS value" do
      env = valid_redis_config(%{"REDIS_TLS" => "invalid"})

      assert_raise ArgumentError, ~r/REDIS_TLS must be true, 1, verify-none, false, or 0/, fn ->
        ConfigParser.redis_config(env)
      end
    end

    test "enables SSL when URL starts with rediss://" do
      env = valid_redis_config(%{"REDIS_URL" => "rediss://localhost:6379"})
      config = ConfigParser.redis_config(env)

      assert config[:url] == "rediss://localhost:6379"
      assert config[:tls] == [verify: :verify_none]
    end

    test "configures TLS with CA certificate" do
      # Create a temporary cert file for testing
      cert_path = create_temp_cert_file()

      env = valid_redis_config(%{"REDIS_TLS_CA_CERT_FILE" => cert_path})
      config = ConfigParser.redis_config(env)

      assert config[:tls] == [verify: :verify_peer, cacertfile: cert_path]

      # Clean up
      File.rm!(cert_path)
    end

    test "configures mTLS with client certificates" do
      # Create temporary cert files for testing
      ca_cert_path = create_temp_cert_file()
      client_cert_path = create_temp_cert_file()
      client_key_path = create_temp_cert_file()

      env =
        valid_redis_config(%{
          "REDIS_TLS_CA_CERT_FILE" => ca_cert_path,
          "REDIS_TLS_CLIENT_CERT_FILE" => client_cert_path,
          "REDIS_TLS_CLIENT_KEY_FILE" => client_key_path
        })

      config = ConfigParser.redis_config(env)

      expected_tls = [
        verify: :verify_peer,
        keyfile: client_key_path,
        certfile: client_cert_path,
        cacertfile: ca_cert_path
      ]

      assert config[:tls] == expected_tls

      # Clean up
      File.rm!(ca_cert_path)
      File.rm!(client_cert_path)
      File.rm!(client_key_path)
    end

    test "allows explicit verify_none with certificates" do
      cert_path = create_temp_cert_file()

      env =
        valid_redis_config(%{
          "REDIS_TLS_CA_CERT_FILE" => cert_path,
          "REDIS_TLS_VERIFY" => "verify-none"
        })

      config = ConfigParser.redis_config(env)

      assert config[:tls] == [verify: :verify_none, cacertfile: cert_path]

      File.rm!(cert_path)
    end

    test "raises error when client cert file doesn't exist" do
      env = valid_redis_config(%{"REDIS_TLS_CA_CERT_FILE" => "/nonexistent/ca.crt"})

      assert_raise ArgumentError, ~r/No file exists for path provided for/, fn ->
        ConfigParser.redis_config(env)
      end
    end

    test "raises error when client cert is provided without key" do
      cert_path = create_temp_cert_file()

      env = valid_redis_config(%{"REDIS_TLS_CLIENT_CERT_FILE" => cert_path})

      assert_raise ArgumentError,
                   ~r/REDIS_TLS_CLIENT_KEY_FILE must be set when REDIS_TLS_CLIENT_CERT_FILE is provided/,
                   fn ->
                     ConfigParser.redis_config(env)
                   end

      File.rm!(cert_path)
    end

    test "raises error when client key is provided without cert" do
      key_path = create_temp_cert_file()

      env = valid_redis_config(%{"REDIS_TLS_CLIENT_KEY_FILE" => key_path})

      assert_raise ArgumentError, ~r/REDIS_TLS_CLIENT_CERT_FILE must be set/, fn ->
        ConfigParser.redis_config(env)
      end

      File.rm!(key_path)
    end

    test "raises error for invalid REDIS_TLS_VERIFY value" do
      cert_path = create_temp_cert_file()

      env =
        valid_redis_config(%{
          "REDIS_TLS_CA_CERT_FILE" => cert_path,
          "REDIS_TLS_VERIFY" => "invalid_mode"
        })

      assert_raise ArgumentError, ~r/REDIS_TLS_VERIFY must be either 'verify-peer' or 'verify-none'/, fn ->
        ConfigParser.redis_config(env)
      end

      File.rm!(cert_path)
    end

    test "REDIS_TLS=false disables TLS even with cert files" do
      cert_path = create_temp_cert_file()

      env =
        valid_redis_config(%{
          "REDIS_TLS" => "false",
          "REDIS_TLS_CA_CERT_FILE" => cert_path
        })

      assert_raise ArgumentError, ~r/REDIS_TLS must be set to true/, fn ->
        ConfigParser.redis_config(env)
      end

      File.rm!(cert_path)
    end

    test "TLS cert files take precedence over basic REDIS_TLS" do
      cert_path = create_temp_cert_file()

      env =
        valid_redis_config(%{
          "REDIS_TLS" => "true",
          "REDIS_TLS_CA_CERT_FILE" => cert_path
        })

      config = ConfigParser.redis_config(env)

      # Should use proper cert validation, not basic SSL
      assert config[:tls] == [verify: :verify_peer, cacertfile: cert_path]

      File.rm!(cert_path)
    end

    test "backward compatibility for REDIS_SSL: no cert files defaults to verify_none" do
      env = valid_redis_config(%{"REDIS_SSL" => "true"})
      config = ConfigParser.redis_config(env)

      assert config[:tls] == [verify: :verify_none]
    end

    test "backward compatibility for REDIS_TLS: no cert files defaults to verify_none" do
      env = valid_redis_config(%{"REDIS_TLS" => "true"})
      config = ConfigParser.redis_config(env)

      assert config[:tls] == [verify: :verify_none]
    end
  end

  describe "log_level/2" do
    test "returns default provided level when LOG_LEVEL is not set" do
      assert ConfigParser.log_level(%{}, :info) == :info
      assert ConfigParser.log_level(%{}, :debug) == :debug
      assert ConfigParser.log_level(%{}, :warning) == :warning
    end

    test "returns atom level when valid LOG_LEVEL is set" do
      for level <- ["error", "warning", "notice", "info", "debug"] do
        env = %{"LOG_LEVEL" => level}
        expected_atom = String.to_atom(level)
        assert ConfigParser.log_level(env, :info) == expected_atom
      end
    end

    test "returns lowercase atom variants" do
      env = %{"LOG_LEVEL" => "ERROR"}
      assert ConfigParser.log_level(env, :info) == :error

      env = %{"LOG_LEVEL" => "Debug"}
      assert ConfigParser.log_level(env, :info) == :debug
    end

    @tag capture_log: true
    test "returns provided default level for invalid LOG_LEVEL" do
      env = %{"LOG_LEVEL" => "invalid"}
      assert ConfigParser.log_level(env, :info) == :info
      assert ConfigParser.log_level(env, :debug) == :debug
      assert ConfigParser.log_level(env, :warning) == :warning
    end
  end

  describe "secret_key_base/1" do
    test "returns the value when valid SECRET_KEY_BASE is set" do
      # Generate a valid base64 encoded 64-byte string
      valid_secret = 64 |> :crypto.strong_rand_bytes() |> Base.encode64()
      env = %{"SECRET_KEY_BASE" => valid_secret}

      assert ConfigParser.secret_key_base(env) == valid_secret
    end

    test "raises error when SECRET_KEY_BASE is not set" do
      assert_raise ArgumentError, ~r/SECRET_KEY_BASE is not set/, fn ->
        ConfigParser.secret_key_base(%{})
      end
    end

    test "raises error when SECRET_KEY_BASE has incorrect length" do
      too_short = 32 |> :crypto.strong_rand_bytes() |> Base.encode64()

      env = %{"SECRET_KEY_BASE" => too_short}

      assert_raise ArgumentError, ~r/Environment variable SECRET_KEY_BASE is too short/, fn ->
        ConfigParser.secret_key_base(env)
      end
    end
  end

  describe "vault_key/1" do
    test "returns the value when valid VAULT_KEY is set" do
      # Generate a valid base64 encoded 32-byte string
      valid_key = 32 |> :crypto.strong_rand_bytes() |> Base.encode64()
      env = %{"VAULT_KEY" => valid_key}

      assert ConfigParser.vault_key(env) == valid_key
    end

    test "raises error when VAULT_KEY is not set" do
      assert_raise ArgumentError, ~r/VAULT_KEY is not set/, fn ->
        ConfigParser.vault_key(%{})
      end
    end

    test "raises error when VAULT_KEY is not valid base64" do
      env = %{"VAULT_KEY" => "not_valid_base64!@# some padding for length"}

      assert_raise ArgumentError, ~r/VAULT_KEY is not valid base64/, fn ->
        ConfigParser.vault_key(env)
      end
    end

    test "raises error when VAULT_KEY has incorrect length" do
      # Generate a base64 encoded string that's not 32 bytes when decoded
      too_short = 16 |> :crypto.strong_rand_bytes() |> Base.encode64()

      env = %{"VAULT_KEY" => too_short}

      assert_raise ArgumentError, ~r/Environment variable VAULT_KEY is too short/, fn ->
        ConfigParser.vault_key(env)
      end
    end
  end

  defp valid_redis_config(attrs \\ %{}) do
    Map.merge(%{"REDIS_URL" => "redis://localhost:6379"}, attrs)
  end

  defp create_temp_cert_file do
    # Create a temporary file with some dummy content for testing
    path = Path.join(System.tmp_dir!(), "test_cert_#{System.unique_integer()}.crt")
    File.write!(path, "dummy cert content")
    path
  end
end
