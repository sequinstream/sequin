defmodule Sequin.ConfigParserTest do
  use Sequin.Case, async: true

  alias Sequin.ConfigParser

  describe "redis_url/1" do
    test "returns URL when REDIS_URL is set" do
      env = valid_redis_config()
      assert ConfigParser.redis_url(env) == "redis://localhost:6379"
    end

    test "raises error when REDIS_URL is not set" do
      assert_raise RuntimeError, ~r/REDIS_URL is not set/, fn ->
        ConfigParser.redis_url(%{})
      end
    end
  end

  describe "redis_opts/1" do
    test "returns default options when no env vars set" do
      assert ConfigParser.redis_opts(valid_redis_config()) == [socket_opts: []]
    end

    test "configures SSL for valid SSL settings" do
      for ssl_value <- ["true", "1", "verify-none"] do
        env = valid_redis_config(%{"REDIS_SSL" => ssl_value})
        opts = ConfigParser.redis_opts(env)

        assert opts[:ssl] == true
        assert opts[:socket_opts] == [verify: :verify_none]
      end
    end

    test "does not configure SSL for disabled SSL settings" do
      for ssl_value <- ["false", "0"] do
        env = valid_redis_config(%{"REDIS_SSL" => ssl_value})
        assert ConfigParser.redis_opts(env) == [socket_opts: []]
      end
    end

    test "raises error for invalid REDIS_SSL value" do
      env = valid_redis_config(%{"REDIS_SSL" => "invalid"})

      assert_raise RuntimeError, ~r/REDIS_SSL must be true, 1, verify-none, false, or 0/, fn ->
        ConfigParser.redis_opts(env)
      end
    end

    test "enables SSL when URL starts with rediss://" do
      env = valid_redis_config(%{"REDIS_URL" => "rediss://localhost:6379"})
      opts = ConfigParser.redis_opts(env)

      assert opts[:ssl] == true
      assert opts[:socket_opts] == [verify: :verify_none]
    end

    test "configures IPv6 for valid IPv6 settings" do
      for ipv6_value <- ["true", "1"] do
        env = valid_redis_config(%{"REDIS_IPV6" => ipv6_value})
        opts = ConfigParser.redis_opts(env)

        assert :inet6 in opts[:socket_opts]
      end
    end

    test "combines SSL and IPv6 options when both enabled" do
      env =
        valid_redis_config(%{
          "REDIS_SSL" => "true",
          "REDIS_IPV6" => "true"
        })

      opts = ConfigParser.redis_opts(env)

      assert opts[:ssl] == true
      assert :inet6 in opts[:socket_opts]
      assert {:verify, :verify_none} in opts[:socket_opts]
    end
  end

  defp valid_redis_config(attrs \\ %{}) do
    Map.merge(%{"REDIS_URL" => "redis://localhost:6379"}, attrs)
  end
end
