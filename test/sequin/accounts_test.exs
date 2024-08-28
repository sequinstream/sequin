defmodule Sequin.AccountsTest do
  use Sequin.DataCase, async: true

  alias Sequin.Accounts
  alias Sequin.Accounts.User
  alias Sequin.Factory.AccountsFactory

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
      valid_attrs = %{name: "John Doe", email: "john@example.com", account_id: account.id}

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
      attrs = %{name: "New User", email: existing_user.email, account_id: account.id}

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
end
