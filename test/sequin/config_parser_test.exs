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
      assert_raise RuntimeError, ~r/REDIS_URL is not set/, fn ->
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

      assert_raise RuntimeError, ~r/REDIS_SSL must be true, 1, verify-none, false, or 0/, fn ->
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

  describe "log_level/1" do
    test "returns default :info when LOG_LEVEL is not set" do
      assert ConfigParser.log_level(%{}) == :info
    end

    test "returns atom level when valid LOG_LEVEL is set" do
      for level <- ["error", "warning", "notice", "info", "debug"] do
        env = %{"LOG_LEVEL" => level}
        expected_atom = String.to_atom(level)
        assert ConfigParser.log_level(env) == expected_atom
      end
    end

    test "returns uppercase variants" do
      env = %{"LOG_LEVEL" => "ERROR"}
      assert ConfigParser.log_level(env) == :error

      env = %{"LOG_LEVEL" => "Debug"}
      assert ConfigParser.log_level(env) == :debug
    end

    @tag capture_log: true
    test "returns default :info for invalid LOG_LEVEL" do
      env = %{"LOG_LEVEL" => "invalid"}
      assert ConfigParser.log_level(env) == :info
    end
  end

  defp valid_redis_config(attrs \\ %{}) do
    Map.merge(%{"REDIS_URL" => "redis://localhost:6379"}, attrs)
  end
end
