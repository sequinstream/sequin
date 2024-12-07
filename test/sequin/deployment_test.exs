defmodule Sequin.DeploymentTest do
  use Sequin.DataCase, async: true

  alias Sequin.Deployment
  alias Sequin.Deployment.DeploymentConfig
  alias Sequin.Factory

  describe "config/1" do
    test "returns cached value on subsequent calls" do
      test_key = Factory.unique_word()

      # First call should hit the database
      assert nil == Deployment.config(test_key)

      # Set a value
      assert {:ok, _} = Deployment.set_config(test_key, %{value: "test"})

      # First read should hit database
      value = Deployment.config(test_key)
      assert %DeploymentConfig{value: %{"value" => "test"}} = value

      # Kill the database connection to prove we're using cache
      Ecto.Adapters.SQL.Sandbox.checkin(Sequin.Repo)

      # Should return cached value without database
      assert ^value = Deployment.config(test_key)
    end

    test "returns nil for non-existent keys" do
      assert nil == Deployment.config(Factory.unique_word())
    end
  end

  describe "set_config/2" do
    test "creates new config" do
      key = Factory.unique_word()
      assert {:ok, config} = Deployment.set_config(key, %{value: "test"})
      assert config.key == key
      assert config.value == %{value: "test"}
    end

    test "updates existing config and busts cache" do
      test_key = Factory.unique_word()

      # Create initial value
      assert {:ok, _} = Deployment.set_config(test_key, %{value: "initial"})

      # Read it once to cache
      initial = Deployment.config(test_key)
      assert %DeploymentConfig{value: %{"value" => "initial"}} = initial

      # Update the value
      assert {:ok, _} = Deployment.set_config(test_key, %{value: "updated"})

      # Should read new value from database
      updated = Deployment.config(test_key)
      assert %DeploymentConfig{value: %{"value" => "updated"}} = updated
      refute initial == updated
    end

    test "validates input" do
      assert {:error, changeset} = Deployment.set_config(String.duplicate("a", 256), %{})
      assert "should be at most 255 character(s)" in errors_on(changeset).key
    end
  end
end
