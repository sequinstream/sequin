defmodule Sequin.Test.AwsTestHelpers do
  @moduledoc """
  Unified AWS test helpers for credential mocking and setup.

  Provides utilities to easily set up AWS credentials for testing via environment
  variables or mock modules
  """

  alias Sequin.AwsMock
  alias Sequin.Error

  @doc """
  Sets up stub expectations for AwsMock.

  Uses stub instead of expect for more flexible testing.

  ## Options

  - `:access_key_id` - AWS access key ID (default: "AKIATEST123456789")
  - `:secret_access_key` - AWS secret access key (default: "test_secret_key")
  - `:session_token` - AWS session token (optional)
  - `:region` - AWS region (default: "us-east-1")

  ## Example

      setup do
        setup_task_role_stub()
      end
  """
  def setup_task_role_stub(opts \\ []) do
    access_key_id = Keyword.get(opts, :access_key_id, "AKIATEST123456789")
    secret_access_key = Keyword.get(opts, :secret_access_key, "test_secret_key")
    session_token = Keyword.get(opts, :session_token)
    region = Keyword.get(opts, :region, "us-east-1")

    Mox.stub(AwsMock, :get_client, fn _region ->
      client =
        access_key_id
        |> AWS.Client.create(secret_access_key, region)
        |> Sequin.Map.put_if_present(:session_token, session_token)

      {:ok, client}
    end)

    :ok
  end

  @doc """
  Sets up stub expectations for failed AWS credentials.

  Uses stub instead of expect for more flexible testing.

  ## Options

  - `:error_message` - Error message to return (default: "No credentials available")

  ## Example

      setup do
        setup_failed_task_role_stub()
      end
  """
  def setup_failed_task_role_stub do
    Mox.stub(AwsMock, :get_client, fn _ ->
      {:error, Error.service(service: :aws, message: "No credentials available")}
    end)

    :ok
  end
end
