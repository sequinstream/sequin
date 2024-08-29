defmodule Sequin.Accounts.GithubAuthTest do
  use Sequin.DataCase, async: true

  alias Sequin.Accounts
  alias Sequin.Accounts.User
  alias Sequin.Factory.AccountsFactory

  describe "register_user/2 with GitHub provider" do
    test "creates a new user with GitHub provider" do
      attrs = %{
        email: "github_user@example.com",
        name: "GitHub User",
        auth_provider_id: "12345"
      }

      assert {:ok, %User{} = user} = Accounts.register_user(:github, attrs)
      assert user.email == "github_user@example.com"
      assert user.name == "GitHub User"
      assert user.auth_provider == :github
      assert user.auth_provider_id == "12345"
      assert user.hashed_password == nil
    end

    test "returns error changeset with invalid data" do
      attrs = %{email: "invalid", name: nil}
      assert {:error, %Ecto.Changeset{}} = Accounts.register_user(:github, attrs)
    end
  end

  describe "update_user_github_profile/2" do
    setup do
      user = AccountsFactory.insert_user!(auth_provider: :github)
      %{user: user}
    end

    test "updates GitHub user's profile information", %{user: user} do
      attrs = %{name: "Updated Name", email: "updated@example.com"}
      assert {:ok, updated_user} = Accounts.update_user_github_profile(user, attrs)
      assert updated_user.name == "Updated Name"
      assert updated_user.email == "updated@example.com"
    end

    test "does not update other fields", %{user: user} do
      original_auth_provider_id = user.auth_provider_id
      attrs = %{name: "Updated Name", auth_provider_id: "new_id"}
      assert {:ok, updated_user} = Accounts.update_user_github_profile(user, attrs)
      assert updated_user.name == "Updated Name"
      assert updated_user.auth_provider_id == original_auth_provider_id
    end
  end

  describe "get_user_by_email/2 with GitHub provider" do
    test "returns the user with given email for GitHub provider" do
      user = AccountsFactory.insert_user!(auth_provider: :github)
      assert fetched_user = Accounts.get_user_by_email(:github, user.email)
      assert fetched_user.id == user.id
    end

    test "returns nil for non-existent email" do
      assert Accounts.get_user_by_email(:github, "nonexistent@example.com") == nil
    end
  end

  describe "get_user_by_auth_provider_id/2" do
    test "returns the user with given auth_provider_id for GitHub provider" do
      user = AccountsFactory.insert_user!(auth_provider: :github, auth_provider_id: "github_123")
      assert fetched_user = Accounts.get_user_by_auth_provider_id(:github, "github_123")
      assert fetched_user.id == user.id
    end

    test "returns nil for non-existent auth_provider_id" do
      assert Accounts.get_user_by_auth_provider_id(:github, "nonexistent_id") == nil
    end

    # test "returns nil for mismatched auth provider" do
    #   AccountsFactory.insert_user!(auth_provider: :github, auth_provider_id: "github_123")
    #   assert Accounts.get_user_by_auth_provider_id(:google, "github_123") == nil
    # end
  end
end
