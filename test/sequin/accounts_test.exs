defmodule Sequin.AccountsTest do
  use Sequin.DataCase, async: true

  alias Sequin.Accounts
  alias Sequin.Accounts.User
  alias Sequin.Error.NotFoundError
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication.PostgresReplicationSlot

  describe "users" do
    test "list_users/1 returns all users for an account" do
      account = AccountsFactory.insert_account!()
      user1 = AccountsFactory.insert_user!(account_id: account.id)
      user2 = AccountsFactory.insert_user!(account_id: account.id)
      _other_account_user = AccountsFactory.insert_user!()

      users = Accounts.list_users_for_account(account.id)
      assert length(users) == 2
      assert users |> Enum.map(& &1.id) |> Enum.sort() == Enum.sort([user1.id, user2.id])
    end

    test "get_user/1 returns the user with given id" do
      user = AccountsFactory.insert_user!()
      assert {:ok, fetched_user} = Accounts.get_user(user.id)
      assert fetched_user.id == user.id
    end

    test "get_user/1 returns an error for non-existent user" do
      assert {:error, _} = Accounts.get_user(Ecto.UUID.generate())
    end

    test "get_user_by_email/1 returns the user with given email" do
      user = AccountsFactory.insert_user!()
      assert {:ok, fetched_user} = Accounts.get_user_by_email(user.email)
      assert fetched_user.id == user.id
    end

    test "get_user_by_email/1 returns an error for non-existent email" do
      assert {:error, _} = Accounts.get_user_by_email("nonexistent@example.com")
    end

    test "create_user/1 with valid data creates a user" do
      account = AccountsFactory.insert_account!()
      valid_attrs = AccountsFactory.user_attrs(%{name: "John Doe", email: "john@example.com", account_id: account.id})

      assert {:ok, %User{} = user} = Accounts.create_user(valid_attrs)
      assert user.name == "John Doe"
      assert user.email == "john@example.com"
      assert user.account_id == account.id
    end

    test "create_user/1 with invalid data returns error changeset" do
      invalid_attrs = %{name: nil, email: nil, account_id: nil}
      assert {:error, %Ecto.Changeset{}} = Accounts.create_user(invalid_attrs)
    end

    test "update_user/2 with valid data updates the user" do
      user = AccountsFactory.insert_user!()
      update_attrs = %{name: "Jane Doe", email: "jane@example.com"}

      assert {:ok, %User{} = updated_user} = Accounts.update_user(user, update_attrs)
      assert updated_user.name == "Jane Doe"
      assert updated_user.email == "jane@example.com"
    end

    test "update_user/2 with invalid data returns error changeset" do
      user = AccountsFactory.insert_user!()
      invalid_attrs = %{name: nil, email: nil, account_id: nil}
      assert {:error, %Ecto.Changeset{}} = Accounts.update_user(user, invalid_attrs)
      assert {:ok, ^user} = Accounts.get_user(user.id)
    end

    test "delete_user/1 deletes the user" do
      user = AccountsFactory.insert_user!()
      assert {:ok, %User{}} = Accounts.delete_user(user)
      assert {:error, _} = Accounts.get_user(user.id)
    end

    test "change_user/1 returns a user changeset" do
      user = AccountsFactory.insert_user!()
      assert %Ecto.Changeset{} = Accounts.change_user(user)
    end

    test "create_user/1 with duplicate email returns error changeset" do
      account = AccountsFactory.insert_account!()
      existing_user = AccountsFactory.insert_user!(account_id: account.id)
      attrs = AccountsFactory.user_attrs(%{name: "New User", email: existing_user.email, account_id: account.id})

      assert {:error, changeset} = Accounts.create_user(attrs)
      assert {"has already been taken", _} = changeset.errors[:email]
    end

    test "update_user/2 with duplicate email returns error changeset" do
      account = AccountsFactory.insert_account!()
      existing_user = AccountsFactory.insert_user!(account_id: account.id)
      user_to_update = AccountsFactory.insert_user!(account_id: account.id)
      update_attrs = %{email: existing_user.email}

      assert {:error, changeset} = Accounts.update_user(user_to_update, update_attrs)
      assert {"has already been taken", _} = changeset.errors[:email]
    end
  end

  describe "deprovisioning accounts" do
    test "deprovision_account/1 removes all associated resources" do
      account = AccountsFactory.insert_account!()
      AccountsFactory.insert_user!(account_id: account.id)
      AccountsFactory.insert_api_key!(account_id: account.id)
      db = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      replication_slot =
        ReplicationFactory.insert_postgres_replication!(postgres_database_id: db.id, account_id: account.id)

      ConsumersFactory.insert_http_pull_consumer!(account_id: account.id, replication_slot_id: replication_slot.id)

      assert {:ok, _} = Accounts.deprovision_account(account, :i_am_responsible_for_my_actions)

      refute Enum.any?(Repo.all(Accounts.Account))
      refute Enum.any?(Repo.all(Accounts.User))
      refute Enum.any?(Repo.all(Accounts.ApiKey))
      refute Enum.any?(Repo.all(Sequin.Consumers.HttpEndpoint))
      refute Enum.any?(Repo.all(Sequin.Consumers.HttpPushConsumer))
      refute Enum.any?(Repo.all(Sequin.Consumers.HttpPullConsumer))
      refute Enum.any?(Repo.all(PostgresReplicationSlot))
      refute Enum.any?(Repo.all(Sequin.Databases.PostgresDatabase))
    end
  end

  describe "authentication" do
    test "find_or_create_user_from_auth/1 finds existing user" do
      existing_user = AccountsFactory.insert_user!(auth_provider: :github, auth_provider_id: "12345")

      auth = %Ueberauth.Auth{
        provider: :github,
        uid: "12345",
        info: %{name: "John Doe", email: existing_user.email}
      }

      assert {:ok, user} = Accounts.find_or_create_user_from_auth(auth)
      assert user.id == existing_user.id
    end

    test "find_or_create_user_from_auth/1 creates new user when not found" do
      auth = %Ueberauth.Auth{
        provider: :github,
        uid: "67890",
        info: %{name: "Jane Doe", email: "jane@example.com"}
      }

      assert {:ok, user} = Accounts.find_or_create_user_from_auth(auth)
      assert user.name == "Jane Doe"
      assert user.email == "jane@example.com"
      assert user.auth_provider == :github
      assert user.auth_provider_id == "67890"
    end

    test "get_user_by_auth_provider_id/2 returns user when found" do
      user = AccountsFactory.insert_user!(auth_provider: :github, auth_provider_id: "12345")

      assert {:ok, found_user} = Accounts.get_user_by_auth_provider_id(:github, "12345")
      assert found_user.id == user.id
    end

    test "get_user_by_auth_provider_id/2 returns error when user not found" do
      assert {:error, %NotFoundError{}} = Accounts.get_user_by_auth_provider_id(:github, "nonexistent")
    end
  end
end
