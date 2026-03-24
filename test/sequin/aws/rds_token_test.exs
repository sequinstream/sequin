defmodule Sequin.Aws.RdsTokenTest do
  use Sequin.Case, async: true

  alias Sequin.Aws.RdsToken

  describe "generate/5" do
    test "generates a token string" do
      credentials = %{
        access_key_id: "AKIAIOSFODNN7EXAMPLE",
        secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        token: nil
      }

      assert {:ok, token} =
               RdsToken.generate("mydb.abc123.us-east-1.rds.amazonaws.com", 5432, "admin", credentials, "us-east-1")

      # Token should contain the hostname and query params
      assert token =~ "mydb.abc123.us-east-1.rds.amazonaws.com"
      assert token =~ "Action=connect"
      assert token =~ "DBUser=admin"
      # SigV4 signature components
      assert token =~ "X-Amz-Algorithm"
      assert token =~ "X-Amz-Credential"
      assert token =~ "X-Amz-Signature"
    end

    test "generates a token with session token" do
      credentials = %{
        access_key_id: "AKIAIOSFODNN7EXAMPLE",
        secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        token: "FwoGZXIvYXdzEBYaDHqa0AP"
      }

      assert {:ok, token} =
               RdsToken.generate("mydb.abc123.us-east-1.rds.amazonaws.com", 5432, "admin", credentials, "us-east-1")

      assert token =~ "X-Amz-Security-Token"
    end

    test "URL-encodes special characters in username" do
      credentials = %{
        access_key_id: "AKIAIOSFODNN7EXAMPLE",
        secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        token: nil
      }

      assert {:ok, token} =
               RdsToken.generate("mydb.abc123.us-east-1.rds.amazonaws.com", 5432, "user@domain", credentials, "us-east-1")

      assert token =~ "DBUser=user%40domain"
    end
  end
end
