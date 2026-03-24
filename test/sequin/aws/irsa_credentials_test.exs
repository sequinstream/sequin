defmodule Sequin.Aws.IrsaCredentialsTest do
  use Sequin.Case, async: true

  alias Sequin.Aws.IrsaCredentials

  @role_arn "arn:aws:iam::123456789012:role/test-role"

  describe "init/1" do
    test "returns :ignore when env vars are not set" do
      assert :ignore = IrsaCredentials.init([])
    end

    test "returns :ignore when only role_arn is set" do
      assert :ignore = IrsaCredentials.init(role_arn: @role_arn)
    end

    test "returns :ignore when only token_file is set" do
      assert :ignore = IrsaCredentials.init(token_file: "/tmp/token")
    end
  end

  describe "init/1 with token file" do
    setup do
      token_file = Path.join(System.tmp_dir!(), "irsa_test_token_#{System.unique_integer([:positive])}")
      File.write!(token_file, "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.test-token")
      on_exit(fn -> File.rm(token_file) end)
      %{token_file: token_file}
    end

    @tag capture_log: true
    test "starts and schedules retry when STS call fails", %{token_file: token_file} do
      # The init will fail the STS call but should start with a retry timer
      {:ok, state} = IrsaCredentials.init(role_arn: @role_arn, token_file: token_file, session_name: "test")

      # Credentials should be nil since STS call failed
      assert state.credentials == nil
      # But a retry timer should be scheduled
      assert state.refresh_timer
    end
  end

  describe "available?/0" do
    test "returns false when GenServer is not running" do
      refute IrsaCredentials.available?()
    end
  end

  describe "get_credentials/0" do
    test "returns error when GenServer is not running" do
      assert {:error, _} = IrsaCredentials.get_credentials()
    end
  end
end
