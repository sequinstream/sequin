defmodule Sequin.AccountsTest do
  use Sequin.DataCase, async: true

  alias Sequin.Accounts
  alias Sequin.Accounts.AccountUser
  alias Sequin.Accounts.User
  alias Sequin.Accounts.UserToken
  alias Sequin.Error.InvariantError
  alias Sequin.Error.NotFoundError
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ApiTokensFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Factory.ReplicationFactory
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Test.Support.AccountsSupport

  describe "users" do
    test "list_users_for_account/1 returns all users for an account" do
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
      assert fetched_user = Accounts.get_user_by_email(:identity, user.email)
      assert fetched_user.id == user.id
    end

    test "get_user_by_email/1 returns nil for non-existent email" do
      assert Accounts.get_user_by_email(:identity, "nonexistent@example.com") == nil
    end

    test "registers a user with identity provider" do
      valid_attrs = AccountsFactory.user_attrs(%{name: "John Doe", email: "john@example.com"})

      assert {:ok, %User{} = user} = Accounts.register_user(:identity, valid_attrs)
      assert user.name == "John Doe"
      assert user.email == "john@example.com"
      assert user.auth_provider == :identity
      assert user.hashed_password != nil
    end

    test "registers a user with GitHub provider" do
      valid_attrs = %{
        name: "Jane Doe",
        email: "jane@example.com",
        auth_provider_id: "github123",
        extra: %{"avatar_url" => "https://example.com/avatar.jpg"}
      }

      assert {:ok, %User{} = user} = Accounts.register_user(:github, valid_attrs)
      assert user.name == "Jane Doe"
      assert user.email == "jane@example.com"
      assert user.auth_provider == :github
      assert user.auth_provider_id == "github123"
      assert user.hashed_password == nil
      assert user.extra == %{"avatar_url" => "https://example.com/avatar.jpg"}
    end

    test "returns error changeset with invalid data" do
      invalid_attrs = %{name: nil, email: nil, password: nil, account_id: nil}
      assert {:error, %Ecto.Changeset{}} = Accounts.register_user(:identity, invalid_attrs)
    end

    test "update_user/2 with valid data updates the user" do
      user = AccountsFactory.insert_user!()
      update_attrs = %{name: "Jane Doe"}

      assert {:ok, %User{} = updated_user} = Accounts.update_user(user, update_attrs)
      assert updated_user.name == "Jane Doe"
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
      existing_user = AccountsFactory.insert_user!()
      attrs = AccountsFactory.user_attrs(%{email: existing_user.email})

      assert {:error, changeset} = Accounts.register_user(:identity, attrs)
      assert {"has already been taken", _} = changeset.errors[:email]
    end
  end

  describe "deprovisioning accounts" do
    test "delete_account_and_account_resources/2 removes all associated resources" do
      account = AccountsFactory.insert_account!()
      AccountsFactory.insert_user!(account_id: account.id)
      ApiTokensFactory.insert_token!(account_id: account.id)
      db = DatabasesFactory.insert_postgres_database!(account_id: account.id)

      replication_slot =
        ReplicationFactory.insert_postgres_replication!(postgres_database_id: db.id, account_id: account.id)

      ConsumersFactory.insert_http_pull_consumer!(
        account_id: account.id,
        postgres_database_id: db.id,
        replication_slot_id: replication_slot.id
      )

      assert {:ok, _} = Accounts.delete_account_and_account_resources(account, delete_users: true)

      refute Enum.any?(Repo.all(Accounts.Account))
      refute Enum.any?(Repo.all(Accounts.User))
      refute Enum.any?(Repo.all(Sequin.ApiTokens.ApiToken))
      refute Enum.any?(Repo.all(Sequin.Consumers.HttpEndpoint))
      refute Enum.any?(Repo.all(Sequin.Consumers.HttpPushConsumer))
      refute Enum.any?(Repo.all(Sequin.Consumers.HttpPullConsumer))
      refute Enum.any?(Repo.all(PostgresReplicationSlot))
      refute Enum.any?(Repo.all(Sequin.Databases.PostgresDatabase))
    end
  end

  describe "get_user_by_email_and_password/2" do
    test "does not return the user if the email does not exist" do
      refute Accounts.get_user_by_email_and_password("unknown@example.com", "hello world!")
    end

    test "does not return the user if the password is not valid" do
      user = AccountsFactory.insert_user!()
      refute Accounts.get_user_by_email_and_password(user.email, "invalid")
    end

    test "returns the user if the email and password are valid" do
      %{id: id} = user = AccountsFactory.insert_user!()

      assert %User{id: ^id} =
               Accounts.get_user_by_email_and_password(user.email, user.password)
    end
  end

  describe "register_user/1" do
    test "requires email and password to be set" do
      {:error, changeset} = Accounts.register_user(:identity, %{})

      assert %{
               password: ["can't be blank"],
               email: ["can't be blank"]
             } = errors_on(changeset)
    end

    test "validates email and password when given" do
      {:error, changeset} = Accounts.register_user(:identity, %{email: "not valid", password: "not valid"})

      assert %{
               email: ["must have the @ sign and no spaces"],
               password: ["should be at least 12 character(s)"]
             } = errors_on(changeset)
    end

    test "validates maximum values for email and password for security" do
      too_long = String.duplicate("db", 100)
      {:error, changeset} = Accounts.register_user(:identity, %{email: too_long, password: too_long})
      assert "should be at most 160 character(s)" in errors_on(changeset).email
      assert "should be at most 72 character(s)" in errors_on(changeset).password
    end

    test "validates email uniqueness" do
      %{email: email} = AccountsFactory.insert_user!()
      {:error, changeset} = Accounts.register_user(:identity, %{email: email})
      assert "has already been taken" in errors_on(changeset).email

      # Now try with the upper cased email too, to check that email case is ignored.
      {:error, changeset} = Accounts.register_user(:identity, %{email: String.upcase(email)})
      assert "has already been taken" in errors_on(changeset).email
    end

    test "registers users with a hashed password" do
      email = "user#{System.unique_integer()}@example.com"
      {:ok, user} = Accounts.register_user(:identity, %{email: email, password: "valid_password12"})
      assert user.email == email
      assert is_binary(user.hashed_password)
      assert is_nil(user.confirmed_at)
      assert is_nil(user.password)
    end
  end

  describe "change_user_registration/2" do
    test "returns a changeset" do
      assert %Ecto.Changeset{} = changeset = Accounts.change_user_registration(%User{})
      assert changeset.required == [:password, :email]
    end

    test "allows fields to be set" do
      email = "user#{System.unique_integer()}@example.com"
      password = "valid_password12"

      changeset =
        Accounts.change_user_registration(
          %User{},
          %{"email" => email, "password" => password}
        )

      assert changeset.valid?
      assert get_change(changeset, :email) == email
      assert get_change(changeset, :password) == password
      assert is_nil(get_change(changeset, :hashed_password))
    end
  end

  describe "change_user_email/2" do
    test "returns a user changeset" do
      assert %Ecto.Changeset{} = changeset = Accounts.change_user_email(%User{auth_provider: :identity})
      assert changeset.required == [:email]
    end
  end

  describe "apply_user_email/3" do
    setup do
      %{user: AccountsFactory.insert_user!()}
    end

    test "requires email to change", %{user: user} do
      {:error, changeset} = Accounts.apply_user_email(user, "invalid", %{})
      assert %{email: ["did not change"]} = errors_on(changeset)
    end

    test "validates email", %{user: user} do
      {:error, changeset} =
        Accounts.apply_user_email(user, "invalid", %{email: "not valid"})

      assert %{email: ["must have the @ sign and no spaces"]} = errors_on(changeset)
    end

    test "validates maximum value for email for security", %{user: user} do
      too_long = String.duplicate("db", 100)

      {:error, changeset} =
        Accounts.apply_user_email(user, "invalid", %{email: too_long})

      assert "should be at most 160 character(s)" in errors_on(changeset).email
    end

    test "validates email uniqueness", %{user: user} do
      %{email: email} = AccountsFactory.insert_user!()
      password = AccountsFactory.password()

      {:error, changeset} = Accounts.apply_user_email(user, password, %{email: email})

      assert "has already been taken" in errors_on(changeset).email
    end

    test "validates current password", %{user: user} do
      {:error, changeset} =
        Accounts.apply_user_email(user, "invalid", %{email: "valid@email.com"})

      assert %{current_password: ["is not valid"]} = errors_on(changeset)
    end

    test "applies the email without persisting it", %{user: user} do
      email = "valid@email.com"
      {:ok, user} = Accounts.apply_user_email(user, user.password, %{email: email})
      assert user.email == email
      assert Accounts.get_user!(user.id).email != email
    end
  end

  describe "deliver_user_update_email_instructions/3" do
    setup do
      %{user: AccountsFactory.insert_user!()}
    end

    test "sends token through notification", %{user: user} do
      token =
        AccountsSupport.extract_user_token(fn url ->
          Accounts.deliver_user_update_email_instructions(user, "current@example.com", url)
        end)

      {:ok, token} = Base.url_decode64(token, padding: false)
      assert user_token = Repo.get_by(UserToken, token: :crypto.hash(:sha256, token))
      assert user_token.user_id == user.id
      assert user_token.sent_to == user.email
      assert user_token.context == "change:current@example.com"
    end
  end

  describe "update_user_email/2" do
    setup do
      user = AccountsFactory.insert_user!()
      email = "newemail@example.com"

      token =
        AccountsSupport.extract_user_token(fn url ->
          Accounts.deliver_user_update_email_instructions(%{user | email: email}, user.email, url)
        end)

      %{user: user, token: token, email: email}
    end

    test "updates the email with a valid token", %{user: user, token: token, email: email} do
      assert Accounts.update_user_email(user, token) == :ok
      changed_user = Repo.get!(User, user.id)
      assert changed_user.email != user.email
      assert changed_user.email == email
      assert changed_user.confirmed_at
      assert changed_user.confirmed_at != user.confirmed_at
      refute Repo.get_by(UserToken, user_id: user.id)
    end

    test "does not update email with invalid token", %{user: user} do
      assert Accounts.update_user_email(user, "oops") == :error
      assert Repo.get!(User, user.id).email == user.email
      assert Repo.get_by(UserToken, user_id: user.id)
    end

    test "does not update email if user email changed", %{user: user, token: token} do
      assert Accounts.update_user_email(%{user | email: "current@example.com"}, token) == :error
      assert Repo.get!(User, user.id).email == user.email
      assert Repo.get_by(UserToken, user_id: user.id)
    end

    test "does not update email if token expired", %{user: user, token: token} do
      {1, nil} = Repo.update_all(UserToken, set: [inserted_at: ~N[2020-01-01 00:00:00]])
      assert Accounts.update_user_email(user, token) == :error
      assert Repo.get!(User, user.id).email == user.email
      assert Repo.get_by(UserToken, user_id: user.id)
    end
  end

  describe "change_user_password/2" do
    test "returns a user changeset" do
      assert %Ecto.Changeset{} = changeset = Accounts.change_user_password(%User{auth_provider: :identity})
      assert changeset.required == [:password]
    end

    test "allows fields to be set" do
      changeset =
        Accounts.change_user_password(%User{auth_provider: :identity}, %{
          "password" => "new valid password"
        })

      assert changeset.valid?
      assert get_change(changeset, :password) == "new valid password"
      assert is_nil(get_change(changeset, :hashed_password))
    end
  end

  describe "update_user_password/3" do
    setup do
      %{user: AccountsFactory.insert_user!()}
    end

    test "validates password", %{user: user} do
      {:error, changeset} =
        Accounts.update_user_password(user, AccountsFactory.password(), %{
          password: "not valid",
          password_confirmation: "another"
        })

      assert %{
               password: ["should be at least 12 character(s)"],
               password_confirmation: ["does not match password"]
             } = errors_on(changeset)
    end

    test "validates maximum values for password for security", %{user: user} do
      too_long = String.duplicate("db", 100)

      {:error, changeset} =
        Accounts.update_user_password(user, AccountsFactory.password(), %{password: too_long})

      assert "should be at most 72 character(s)" in errors_on(changeset).password
    end

    test "validates current password", %{user: user} do
      {:error, changeset} =
        Accounts.update_user_password(user, "invalid", %{password: AccountsFactory.password()})

      assert %{current_password: ["is not valid"]} = errors_on(changeset)
    end

    test "updates the password", %{user: user} do
      password = user.password
      user = Map.put(user, :password, nil)

      {:ok, user} =
        Accounts.update_user_password(user, password, %{
          password: "new valid password"
        })

      assert is_nil(user.password)
      assert Accounts.get_user_by_email_and_password(user.email, "new valid password")
    end

    test "deletes all tokens for the given user", %{user: user} do
      _ = Accounts.generate_user_session_token(user)

      {:ok, _} =
        Accounts.update_user_password(user, user.password, %{
          password: "new valid password"
        })

      refute Repo.get_by(UserToken, user_id: user.id)
    end
  end

  describe "generate_user_session_token/1" do
    setup do
      %{user: AccountsFactory.insert_user!()}
    end

    test "generates a token", %{user: user} do
      token = Accounts.generate_user_session_token(user)
      assert user_token = Repo.get_by(UserToken, token: token)
      assert user_token.context == "session"

      # Creating the same token for another user should fail
      assert_raise Ecto.ConstraintError, fn ->
        Repo.insert!(%UserToken{
          token: user_token.token,
          user_id: AccountsFactory.insert_user!().id,
          context: "session"
        })
      end
    end
  end

  describe "get_user_by_session_token/1" do
    setup do
      user = AccountsFactory.insert_user!()
      token = Accounts.generate_user_session_token(user)
      %{user: user, token: token}
    end

    test "returns user by token", %{user: user, token: token} do
      assert session_user = Accounts.get_user_by_session_token(token)
      assert session_user.id == user.id
    end

    test "does not return user for invalid token" do
      refute Accounts.get_user_by_session_token("oops")
    end

    test "does not return user for expired token", %{token: token} do
      {1, nil} = Repo.update_all(UserToken, set: [inserted_at: ~N[2020-01-01 00:00:00]])
      refute Accounts.get_user_by_session_token(token)
    end
  end

  describe "delete_user_session_token/1" do
    test "deletes the token" do
      user = AccountsFactory.insert_user!()
      token = Accounts.generate_user_session_token(user)
      assert Accounts.delete_user_session_token(token) == :ok
      refute Accounts.get_user_by_session_token(token)
    end
  end

  describe "deliver_user_confirmation_instructions/2" do
    setup do
      %{user: AccountsFactory.insert_user!()}
    end

    test "sends token through notification", %{user: user} do
      token =
        AccountsSupport.extract_user_token(fn url ->
          Accounts.deliver_user_confirmation_instructions(user, url)
        end)

      {:ok, token} = Base.url_decode64(token, padding: false)
      assert user_token = Repo.get_by(UserToken, token: :crypto.hash(:sha256, token))
      assert user_token.user_id == user.id
      assert user_token.sent_to == user.email
      assert user_token.context == "confirm"
    end
  end

  describe "confirm_user/1" do
    setup do
      user = AccountsFactory.insert_user!()

      token =
        AccountsSupport.extract_user_token(fn url ->
          Accounts.deliver_user_confirmation_instructions(user, url)
        end)

      %{user: user, token: token}
    end

    test "confirms the email with a valid token", %{user: user, token: token} do
      assert {:ok, confirmed_user} = Accounts.confirm_user(token)
      assert confirmed_user.confirmed_at
      assert confirmed_user.confirmed_at != user.confirmed_at
      assert Repo.get!(User, user.id).confirmed_at
      refute Repo.get_by(UserToken, user_id: user.id)
    end

    test "does not confirm with invalid token", %{user: user} do
      assert Accounts.confirm_user("oops") == :error
      refute Repo.get!(User, user.id).confirmed_at
      assert Repo.get_by(UserToken, user_id: user.id)
    end

    test "does not confirm email if token expired", %{user: user, token: token} do
      {1, nil} = Repo.update_all(UserToken, set: [inserted_at: ~N[2020-01-01 00:00:00]])
      assert Accounts.confirm_user(token) == :error
      refute Repo.get!(User, user.id).confirmed_at
      assert Repo.get_by(UserToken, user_id: user.id)
    end
  end

  describe "deliver_user_reset_password_instructions/2" do
    setup do
      %{user: AccountsFactory.insert_user!()}
    end

    test "sends token through notification", %{user: user} do
      token =
        AccountsSupport.extract_user_token(fn url ->
          Accounts.deliver_user_reset_password_instructions(user, url)
        end)

      {:ok, token} = Base.url_decode64(token, padding: false)
      assert user_token = Repo.get_by(UserToken, token: :crypto.hash(:sha256, token))
      assert user_token.user_id == user.id
      assert user_token.sent_to == user.email
      assert user_token.context == "reset_password"
    end
  end

  describe "get_user_by_reset_password_token/1" do
    setup do
      user = AccountsFactory.insert_user!()

      token =
        AccountsSupport.extract_user_token(fn url ->
          Accounts.deliver_user_reset_password_instructions(user, url)
        end)

      %{user: user, token: token}
    end

    test "returns the user with valid token", %{user: %{id: id}, token: token} do
      assert %User{id: ^id} = Accounts.get_user_by_reset_password_token(token)
      assert Repo.get_by(UserToken, user_id: id)
    end

    test "does not return the user with invalid token", %{user: user} do
      refute Accounts.get_user_by_reset_password_token("oops")
      assert Repo.get_by(UserToken, user_id: user.id)
    end

    test "does not return the user if token expired", %{user: user, token: token} do
      {1, nil} = Repo.update_all(UserToken, set: [inserted_at: ~N[2020-01-01 00:00:00]])
      refute Accounts.get_user_by_reset_password_token(token)
      assert Repo.get_by(UserToken, user_id: user.id)
    end
  end

  describe "reset_user_password/2" do
    setup do
      %{user: AccountsFactory.insert_user!()}
    end

    test "validates password", %{user: user} do
      {:error, changeset} =
        Accounts.reset_user_password(user, %{
          password: "not valid",
          password_confirmation: "another"
        })

      assert %{
               password: ["should be at least 12 character(s)"],
               password_confirmation: ["does not match password"]
             } = errors_on(changeset)
    end

    test "validates maximum values for password for security", %{user: user} do
      too_long = String.duplicate("db", 100)
      {:error, changeset} = Accounts.reset_user_password(user, %{password: too_long})
      assert "should be at most 72 character(s)" in errors_on(changeset).password
    end

    test "updates the password", %{user: user} do
      # Factory returns a user with a password on it
      user = Map.put(user, :password, nil)
      {:ok, updated_user} = Accounts.reset_user_password(user, %{password: "new valid password"})
      assert is_nil(updated_user.password)
      assert Accounts.get_user_by_email_and_password(user.email, "new valid password")
    end

    test "deletes all tokens for the given user", %{user: user} do
      _ = Accounts.generate_user_session_token(user)
      {:ok, _} = Accounts.reset_user_password(user, %{password: "new valid password"})
      refute Repo.get_by(UserToken, user_id: user.id)
    end
  end

  describe "inspect/2 for the User module" do
    test "does not include password" do
      refute inspect(%User{password: "123456"}) =~ "password: \"123456\""
    end
  end

  describe "provider_registration_changeset/2" do
    test "validates required fields" do
      attrs = %{}
      changeset = User.provider_registration_changeset(%User{}, attrs)

      assert %{
               email: ["can't be blank"],
               auth_provider: ["can't be blank"],
               auth_provider_id: ["can't be blank"]
             } = errors_on(changeset)
    end

    test "validates email format" do
      attrs = AccountsFactory.user_attrs(%{email: "invalid"})
      changeset = User.provider_registration_changeset(%User{}, attrs)
      assert %{email: ["must have the @ sign and no spaces"]} = errors_on(changeset)
    end

    test "creates a valid changeset" do
      attrs = AccountsFactory.user_attrs(%{auth_provider: :github, auth_provider_id: "123"})
      changeset = User.provider_registration_changeset(%User{}, attrs)
      assert changeset.valid?
    end
  end

  describe "current_account/1 for the User module" do
    test "returns the current account for the user" do
      user = AccountsFactory.insert_user!()
      account = user.id |> Accounts.list_accounts_for_user() |> List.first()
      assert Accounts.get_account!(account.id) == User.current_account(user)
    end

    test "returns the oldest account for the user if no current account is set" do
      # Creates a user with 1 account, and then associates them with 2 more accounts

      user = AccountsFactory.insert_user!()

      account_2 = AccountsFactory.insert_account!()
      {:ok, _} = Accounts.associate_user_with_account(user, account_2)
      {:ok, _} = Accounts.set_current_account_for_user(user.id, account_2.id)

      account_3 = AccountsFactory.insert_account!()
      {:ok, _} = Accounts.associate_user_with_account(user, account_3)
      {:ok, _} = Accounts.set_current_account_for_user(user.id, account_3.id)
      # Delete the newest account, to set all accounts to current === false
      {:ok, _} = Accounts.delete_account_and_account_resources(account_3)

      user = Repo.preload(user, [:accounts_users, :accounts])

      oldest_account =
        user.accounts
        |> Enum.sort_by(& &1.inserted_at, DateTime)
        |> List.first()

      assert oldest_account == User.current_account(user)
    end
  end

  describe "set_current_account_for_user/2 for the Accounts module" do
    test "sets the current account for the user" do
      user = AccountsFactory.insert_user!()
      account_2 = AccountsFactory.insert_account!()
      {:ok, _} = Accounts.associate_user_with_account(user, account_2)
      {:ok, user} = Accounts.set_current_account_for_user(user.id, account_2.id)

      assert Accounts.get_account!(account_2.id) == User.current_account(user)
    end

    test "has no effect when account_user is already set to current=true" do
      user = AccountsFactory.insert_user!()
      account = AccountsFactory.insert_account!()
      {:ok, _} = Accounts.associate_user_with_account(user, account)
      {:ok, user} = Accounts.set_current_account_for_user(user.id, account.id)
      assert account == User.current_account(user)
      {:ok, user} = Accounts.set_current_account_for_user(user.id, account.id)
      assert account == User.current_account(user)
    end

    test "returns an error if the account is is not associated to the user" do
      user_1 = AccountsFactory.insert_user!()
      user_2 = AccountsFactory.insert_user!()

      account_for_user_2 = user_2.id |> Accounts.list_accounts_for_user() |> List.first()

      assert {:error, %NotFoundError{entity: :account_user}} =
               Accounts.set_current_account_for_user(user_1.id, account_for_user_2.id)
    end
  end

  describe "associate_user_with_account/2 for the Accounts module" do
    test "associates a user with an account" do
      user = AccountsFactory.insert_user!()
      account_2 = AccountsFactory.insert_account!()
      assert {:ok, _} = Accounts.associate_user_with_account(user, account_2)
      user = Repo.preload(user, [:accounts_users, :accounts])
      assert Enum.member?(user.accounts, account_2)
    end

    test "returns an error if the account is not found" do
      user = AccountsFactory.insert_user!()
      account = AccountsFactory.insert_account!()
      # Delete the account
      Accounts.delete_account(account)
      assert {:error, %NotFoundError{entity: :account}} = Accounts.associate_user_with_account(user, account)
    end
  end

  describe "remove_user_from_account/2 for the Accounts module" do
    test "removes a user from an account" do
      user = AccountsFactory.insert_user!()
      account_2 = AccountsFactory.insert_account!()
      Accounts.associate_user_with_account(user, account_2)
      assert {:ok, _} = Accounts.remove_user_from_account(user, account_2)

      user = Repo.preload(user, [:accounts_users, :accounts])
      refute Enum.member?(user.accounts, account_2)
    end

    test "returns an error if the account is not found" do
      user = AccountsFactory.insert_user!()
      account = AccountsFactory.insert_account!()
      assert {:error, %NotFoundError{entity: :account_user}} = Accounts.remove_user_from_account(user, account)
    end
  end

  describe "get_account_for_user/2 for the Accounts module" do
    test "returns the account for the user" do
      user = AccountsFactory.insert_user!()
      account = AccountsFactory.insert_account!()
      Accounts.associate_user_with_account(user, account)
      assert {:ok, ^account} = Accounts.get_account_for_user(user.id, account.id)
    end

    test "returns nil if the user is not associated with the account provided" do
      user = AccountsFactory.insert_user!()
      account = AccountsFactory.insert_account!()
      assert {:error, %NotFoundError{entity: :account}} = Accounts.get_account_for_user(user.id, account.id)
    end
  end

  describe "feature flags" do
    test "has_feature?/2 correctly checks for features" do
      account = AccountsFactory.insert_account!(%{features: ["feature_a", "feature_b"]})

      assert Accounts.has_feature?(account, "feature_a")
      assert Accounts.has_feature?(account, "feature_b")
      refute Accounts.has_feature?(account, "feature_c")
    end

    test "add_feature/2 adds a new feature" do
      account = AccountsFactory.insert_account!(%{features: ["feature_a"]})

      {:ok, updated_account} = Accounts.add_feature(account, "feature_b")
      assert "feature_b" in updated_account.features
      assert "feature_b" in Repo.reload(updated_account).features
      assert length(updated_account.features) == 2

      # Adding an existing feature doesn't duplicate it
      {:ok, updated_account} = Accounts.add_feature(updated_account, "feature_a")
      assert_lists_equal(updated_account.features, ["feature_a", "feature_b"])
    end

    test "remove_feature/2 removes an existing feature" do
      account = AccountsFactory.insert_account!(%{features: ["feature_a", "feature_b"]})

      {:ok, updated_account} = Accounts.remove_feature(account, "feature_a")
      refute "feature_a" in updated_account.features
      refute "feature_a" in Repo.reload(updated_account).features
      assert length(updated_account.features) == 1

      # Removing a non-existent feature doesn't affect the list
      {:ok, updated_account} = Accounts.remove_feature(updated_account, "feature_c")
      assert length(updated_account.features) == 1
    end
  end

  describe "invite_user/4" do
    test "inviting a user to an account they do not own fails" do
      user = AccountsFactory.insert_user!()
      account = AccountsFactory.insert_account!()

      assert {:error, %InvariantError{message: "Cannot invite user to account"}} =
               Accounts.invite_user(user, account, "test@example.com", fn _ -> "http://example.com" end)
    end

    test "inviting a user to an account they already are invited to fails" do
      user = AccountsFactory.insert_user!()
      user2 = AccountsFactory.insert_user!()
      account = AccountsFactory.insert_account!()

      Accounts.associate_user_with_account(user, account)
      Accounts.associate_user_with_account(user2, account)

      assert {:error, %InvariantError{message: "User already invited to account"}} =
               Accounts.invite_user(user, account, user2.email, fn _ -> "http://example.com" end)
    end

    test "sends an email to the user with a link to accept the invite" do
      user = AccountsFactory.insert_user!()
      account = AccountsFactory.insert_account!()
      Accounts.associate_user_with_account(user, account)

      assert token =
               AccountsSupport.extract_user_token(fn url ->
                 Accounts.invite_user(user, account, "test@example.com", url)
               end)

      {:ok, token} = Base.url_decode64(token, padding: false)
      assert user_token = Repo.get_by(UserToken, token: :crypto.hash(:sha256, token))
      assert user_token.user_id == user.id
      assert user_token.sent_to == "test@example.com"
      assert user_token.context == "account-invite"
      assert user_token.annotations["account_id"] == account.id
    end

    test "sending an invite twice removes the previous invite" do
      user = AccountsFactory.insert_user!()
      account = AccountsFactory.insert_account!()
      Accounts.associate_user_with_account(user, account)

      Accounts.invite_user(user, account, "test@example.com", fn _ -> "http://example.com" end)

      assert token =
               AccountsSupport.extract_user_token(fn url ->
                 Accounts.invite_user(user, account, "test@example.com", url)
               end)

      {:ok, token} = Base.url_decode64(token, padding: false)
      assert first_invite_token = Repo.get_by(UserToken, token: :crypto.hash(:sha256, token))

      Accounts.invite_user(user, account, "test@example.com", fn _ -> "http://example.com" end)

      assert token =
               AccountsSupport.extract_user_token(fn url ->
                 Accounts.invite_user(user, account, "test@example.com", url)
               end)

      {:ok, token} = Base.url_decode64(token, padding: false)
      assert second_invite_token = Repo.get_by(UserToken, token: :crypto.hash(:sha256, token))

      assert first_invite_token.id != second_invite_token.id
    end
  end

  describe "accept_invite/2" do
    setup do
      inviting_user = AccountsFactory.insert_user!()
      account = AccountsFactory.insert_account!()
      Accounts.associate_user_with_account(inviting_user, account)
      invited_email = "invited@example.com"

      token =
        AccountsSupport.extract_user_token(fn url ->
          Accounts.invite_user(inviting_user, account, invited_email, url)
        end)

      %{
        inviting_user: inviting_user,
        account: account,
        invited_email: invited_email,
        token: token
      }
    end

    test "accepts an invite", %{invited_email: invited_email, token: token, account: account} do
      invited_user = AccountsFactory.insert_user!(email: invited_email)
      {:ok, hashed_token} = Base.url_decode64(token, padding: false)
      hashed_token = :crypto.hash(:sha256, hashed_token)

      assert {:ok, :ok} = Accounts.accept_invite(invited_user, hashed_token)

      # Verify that the user is now associated with the account
      assert Repo.get_by(AccountUser, user_id: invited_user.id, account_id: account.id)
    end

    test "returns an error if the token is invalid", %{invited_email: invited_email} do
      invited_user = AccountsFactory.insert_user!(email: invited_email)
      invalid_token = "invalid_token"

      assert {:error, %NotFoundError{}} = Accounts.accept_invite(invited_user, invalid_token)
    end

    test "returns an error if the user's email does not match the invite", %{token: token} do
      different_user = AccountsFactory.insert_user!(email: "different@example.com")
      {:ok, hashed_token} = Base.url_decode64(token, padding: false)
      hashed_token = :crypto.hash(:sha256, hashed_token)

      assert {:error, %NotFoundError{}} = Accounts.accept_invite(different_user, hashed_token)
    end

    test "returns an error if the invitation is expired", %{invited_email: invited_email, token: token} do
      invited_user = AccountsFactory.insert_user!(email: invited_email)
      {:ok, hashed_token} = Base.url_decode64(token, padding: false)
      hashed_token = :crypto.hash(:sha256, hashed_token)

      # Expire the token by updating its inserted_at timestamp
      {1, nil} = Repo.update_all(UserToken, set: [inserted_at: ~N[2020-01-01 00:00:00]])

      assert {:error, %NotFoundError{}} = Accounts.accept_invite(invited_user, hashed_token)
    end
  end
end
