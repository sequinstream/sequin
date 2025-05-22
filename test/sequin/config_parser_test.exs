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
      for ssl_value <- ["true", "1", "verify-none"] do
        env = valid_redis_config(%{"REDIS_SSL" => ssl_value})
        config = ConfigParser.redis_config(env)

        assert config[:url] == "redis://localhost:6379"
        assert config[:tls] == [verify: :verify_none]
      end
    end

    test "does not configure SSL for disabled SSL settings" do
      for ssl_value <- ["false", "0"] do
        env = valid_redis_config(%{"REDIS_SSL" => ssl_value})
        config = ConfigParser.redis_config(env)

        assert config[:url] == "redis://localhost:6379"
        refute config[:tls]
      end
    end

    test "raises error for invalid REDIS_SSL value" do
      env = valid_redis_config(%{"REDIS_SSL" => "invalid"})

      assert_raise ArgumentError, ~r/REDIS_SSL must be true, 1, verify-none, false, or 0/, fn ->
        ConfigParser.redis_config(env)
      end
    end

    test "enables SSL when URL starts with rediss://" do
      env = valid_redis_config(%{"REDIS_URL" => "rediss://localhost:6379"})
      config = ConfigParser.redis_config(env)

      assert config[:url] == "rediss://localhost:6379"
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
end
