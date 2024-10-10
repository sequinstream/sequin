defmodule Sequin.Accounts do
  @moduledoc """
  The Accounts context.
  """

  import Ecto.Query, warn: false

  alias Sequin.Accounts.Account
  alias Sequin.Accounts.AccountUser
  alias Sequin.Accounts.AllocatedBastionPort
  alias Sequin.Accounts.User
  alias Sequin.Accounts.UserNotifier
  alias Sequin.Accounts.UserToken
  alias Sequin.ApiTokens
  alias Sequin.Consumers
  alias Sequin.Databases
  alias Sequin.Error
  alias Sequin.Posthog
  alias Sequin.Replication
  alias Sequin.Repo

  require Logger

  # Add this to the list of alias statements at the top of the file
  @doc """
  Checks if any accounts exist in the database. Used to determin if setup is required during self-hosted.

  Returns `true` if at least one account exists, `false` otherwise.

  ## Examples

      iex> any_accounts?()
      true

      iex> any_accounts?()
      false

  """
  def any_accounts? do
    Repo.exists?(Account)
  end

  @doc """
  Gets a user by email.

  ## Examples

      iex> get_user_by_email(:identity, "foo@example.com")
      %User{}

      iex> get_user_by_email(:identity, "unknown@example.com")
      nil

  """
  def get_user_by_email(auth_provider, email) when is_binary(email) do
    Repo.get_by(User, email: email, auth_provider: auth_provider)
  end

  def get_user_by_auth_provider_id(auth_provider, auth_provider_id) when is_binary(auth_provider_id) do
    Repo.get_by(User, auth_provider_id: auth_provider_id, auth_provider: auth_provider)
  end

  @doc """
  Gets a user by email and password.

  ## Examples

      iex> get_user_by_email_and_password("foo@example.com", "correct_password")
      %User{}

      iex> get_user_by_email_and_password("foo@example.com", "invalid_password")
      nil

  """
  def get_user_by_email_and_password(email, password) when is_binary(email) and is_binary(password) do
    user = Repo.get_by(User, email: email, auth_provider: :identity)
    if User.valid_password?(user, password), do: user
  end

  @doc """
  Gets a single user.

  Raises `Ecto.NoResultsError` if the User does not exist.

  ## Examples

      iex> get_user!(123)
      %User{}

      iex> get_user!(456)
      ** (Ecto.NoResultsError)

  """
  def get_user!(id), do: Repo.get!(User, id)

  def get_user_with_preloads!(user_id) do
    User
    |> Repo.get!(user_id)
    |> Repo.preload([:accounts_users, :accounts])
  end

  ## User registration

  @doc """
  Registers a user.

  ## Examples

      iex> register_user(:identity, %{field: value})
      {:ok, %User{}}

      iex> register_user(:github, %{field: value})
      {:ok, %User{}}

      iex> register_user(:identity, %{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def register_user(:identity, attrs) do
    Repo.transact(fn ->
      with {:ok, account} <- create_account(%{}),
           {:ok, user} <-
             %User{}
             |> User.registration_changeset(attrs)
             |> Repo.insert(),
           {:ok, _user_account} <- associate_user_with_account(user, account) do
        broadcast_signup(user, account)
        {:ok, user}
      end
    end)
  end

  def register_user(auth_provider, attrs) do
    Repo.transact(fn ->
      with {:ok, account} <- create_account(%{}),
           {:ok, user} <-
             %User{}
             |> User.provider_registration_changeset(Map.put(attrs, :auth_provider, auth_provider))
             |> Repo.insert(),
           {:ok, _user_account} <- associate_user_with_account(user, account) do
        broadcast_signup(user, account)
        {:ok, user}
      end
    end)
  end

  defp broadcast_signup(user, account) do
    # Record "User Signed Up" event in PostHog
    Posthog.capture("User Signed Up", %{
      distinct_id: user.id,
      properties: %{
        email: user.email,
        auth_provider: user.auth_provider,
        "$groups": %{account: account.id}
      }
    })

    # Trigger Retool workflow to trigger ops tasks
    Req.post(
      "https://api.tryretool.com/v1/workflows/45a5dd7c-6517-4c0b-8c42-1a9388959e8c/startTrigger",
      json: %{
        type: "user_registration",
        data: [user, account]
      },
      headers: [
        {"X-Workflow-Api-Key", Application.fetch_env!(:sequin, :retool_workflow_key)},
        {"Content-Type", "application/json"}
      ]
    )

    # Ensure broadcase NEVER stalls a new user registration
  rescue
    err ->
      Logger.error("Failed to broadcast signup for user #{user.id}. Error: #{inspect(err)}")
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking user changes.

  ## Examples

      iex> change_user_registration(user)
      %Ecto.Changeset{data: %User{}}

  """
  def change_user_registration(%User{} = user, attrs \\ %{}) do
    User.registration_changeset(user, attrs, hash_password: false, validate_email: false)
  end

  ## Settings

  @doc """
  Returns an `%Ecto.Changeset{}` for changing the user email.

  ## Examples

      iex> change_user_email(user)
      %Ecto.Changeset{data: %User{}}

  """
  def change_user_email(%User{auth_provider: :identity} = user, attrs \\ %{}) do
    User.email_changeset(user, attrs, validate_email: false)
  end

  @doc """
  Emulates that the email will change without actually changing
  it in the database.

  ## Examples

      iex> apply_user_email(user, "valid password", %{email: ...})
      {:ok, %User{}}

      iex> apply_user_email(user, "invalid password", %{email: ...})
      {:error, %Ecto.Changeset{}}

  """
  def apply_user_email(%User{auth_provider: :identity} = user, password, attrs) do
    user
    |> User.email_changeset(attrs)
    |> User.validate_current_password(password)
    |> Ecto.Changeset.apply_action(:update)
  end

  @doc """
  Updates the user email using the given token.

  If the token matches, the user email is updated and the token is deleted.
  The confirmed_at date is also updated to the current time.
  """
  def update_user_email(%User{auth_provider: :identity} = user, token) do
    context = "change:#{user.email}"

    with {:ok, query} <- UserToken.verify_change_email_token_query(token, context),
         %UserToken{sent_to: email} <- Repo.one(query),
         {:ok, _} <- Repo.transaction(user_email_multi(user, email, context)) do
      :ok
    else
      _ -> :error
    end
  end

  defp user_email_multi(%User{auth_provider: :identity} = user, email, context) do
    changeset =
      user
      |> User.email_changeset(%{email: email})
      |> User.confirm_changeset()

    Ecto.Multi.new()
    |> Ecto.Multi.update(:user, changeset)
    |> Ecto.Multi.delete_all(:tokens, UserToken.by_user_and_contexts_query(user, [context]))
  end

  @doc ~S"""
  Delivers the update email instructions to the given user.

  ## Examples

      iex> deliver_user_update_email_instructions(user, current_email, &url(~p"/users/settings/confirm_email/#{&1}"))
      {:ok, %{to: ..., body: ...}}

  """
  def deliver_user_update_email_instructions(%User{auth_provider: :identity} = user, current_email, update_email_url_fun)
      when is_function(update_email_url_fun, 1) do
    {encoded_token, user_token} = UserToken.build_email_token(user, "change:#{current_email}")

    Repo.insert!(user_token)
    UserNotifier.deliver_update_email_instructions(user, update_email_url_fun.(encoded_token))
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for changing the user password.

  ## Examples

      iex> change_user_password(user)
      %Ecto.Changeset{data: %User{}}

  """
  def change_user_password(%User{auth_provider: :identity} = user, attrs \\ %{}) do
    User.password_changeset(user, attrs, hash_password: false)
  end

  @doc """
  Updates the user password.

  ## Examples

      iex> update_user_password(user, "valid password", %{password: ...})
      {:ok, %User{}}

      iex> update_user_password(user, "invalid password", %{password: ...})
      {:error, %Ecto.Changeset{}}

  """
  def update_user_password(%User{auth_provider: :identity} = user, password, attrs) do
    changeset =
      user
      |> User.password_changeset(attrs)
      |> User.validate_current_password(password)

    Ecto.Multi.new()
    |> Ecto.Multi.update(:user, changeset)
    |> Ecto.Multi.delete_all(:tokens, UserToken.by_user_and_contexts_query(user, :all))
    |> Repo.transaction()
    |> case do
      {:ok, %{user: user}} -> {:ok, user}
      {:error, :user, changeset, _} -> {:error, changeset}
    end
  end

  ## Session

  @doc """
  Generates a session token.
  """
  def generate_user_session_token(user) do
    {token, user_token} = UserToken.build_session_token(user)
    Repo.insert!(user_token)
    token
  end

  @doc """
  Gets the user with the given signed token.
  """
  def get_user_by_session_token(token) do
    {:ok, query} = UserToken.verify_session_token_query(token)

    query
    |> Repo.one()
    |> case do
      {user, _} -> Repo.preload(user, [:accounts_users, :accounts])
      nil -> nil
    end
  end

  @doc """
  Deletes the signed token with the given context.
  """
  def delete_user_session_token(token) do
    Repo.delete_all(UserToken.by_token_and_context_query(token, "session"))
    :ok
  end

  @doc """
  Generates an impersonation token for a user.
  """
  def generate_impersonation_token(user, account_id) do
    {token, user_token} = UserToken.build_impersonation_token(user, account_id)
    Repo.insert!(user_token)
    token
  end

  @doc """
  Gets the user with the given impersonation token.
  """
  def get_user_by_impersonation_token(token) do
    {:ok, query} = UserToken.verify_session_token_query(token, "impersonate")

    query
    |> Repo.one()
    |> Repo.preload(:account)
  end

  def get_impersonated_account_id(user, token) do
    user_id = user.id

    case UserToken.verify_session_token_query(token, "impersonate") do
      {:ok, query} ->
        case Repo.one(query) do
          {%User{id: ^user_id}, account_id} -> {:ok, account_id}
          _ -> :error
        end
    end
  end

  ## Confirmation

  @doc ~S"""
  Delivers the confirmation email instructions to the given user.

  ## Examples

      iex> deliver_user_confirmation_instructions(user, &url(~p"/users/confirm/#{&1}"))
      {:ok, %{to: ..., body: ...}}

      iex> deliver_user_confirmation_instructions(confirmed_user, &url(~p"/users/confirm/#{&1}"))
      {:error, :already_confirmed}

  """
  def deliver_user_confirmation_instructions(%User{} = user, confirmation_url_fun)
      when is_function(confirmation_url_fun, 1) do
    if user.confirmed_at do
      {:error, :already_confirmed}
    else
      {encoded_token, user_token} = UserToken.build_email_token(user, "confirm")
      Repo.insert!(user_token)
      UserNotifier.deliver_confirmation_instructions(user, confirmation_url_fun.(encoded_token))
    end
  end

  @doc """
  Confirms a user by the given token.

  If the token matches, the user account is marked as confirmed
  and the token is deleted.
  """
  def confirm_user(token) do
    with {:ok, query} <- UserToken.verify_email_token_query(token, "confirm"),
         %User{} = user <- Repo.one(query),
         {:ok, %{user: user}} <- Repo.transaction(confirm_user_multi(user)) do
      {:ok, user}
    else
      _ -> :error
    end
  end

  defp confirm_user_multi(user) do
    Ecto.Multi.new()
    |> Ecto.Multi.update(:user, User.confirm_changeset(user))
    |> Ecto.Multi.delete_all(:tokens, UserToken.by_user_and_contexts_query(user, ["confirm"]))
  end

  ## Reset password

  @doc ~S"""
  Delivers the reset password email to the given user.

  ## Examples

      iex> deliver_user_reset_password_instructions(user, &url(~p"/users/reset_password/#{&1}"))
      {:ok, %{to: ..., body: ...}}

  """
  def deliver_user_reset_password_instructions(%User{auth_provider: :identity} = user, reset_password_url_fun)
      when is_function(reset_password_url_fun, 1) do
    {encoded_token, user_token} = UserToken.build_email_token(user, "reset_password")
    Repo.insert!(user_token)
    UserNotifier.deliver_reset_password_instructions(user, reset_password_url_fun.(encoded_token))
  end

  @doc """
  Gets the user by reset password token.

  ## Examples

      iex> get_user_by_reset_password_token("validtoken")
      %User{}

      iex> get_user_by_reset_password_token("invalidtoken")
      nil

  """
  def get_user_by_reset_password_token(token) do
    with {:ok, query} <- UserToken.verify_email_token_query(token, "reset_password"),
         %User{} = user <- Repo.one(query) do
      user
    else
      _ -> nil
    end
  end

  @doc """
  Resets the user password.

  ## Examples

      iex> reset_user_password(user, %{password: "new long password", password_confirmation: "new long password"})
      {:ok, %User{}}

      iex> reset_user_password(user, %{password: "valid", password_confirmation: "not the same"})
      {:error, %Ecto.Changeset{}}

  """
  def reset_user_password(%User{auth_provider: :identity} = user, attrs) do
    Ecto.Multi.new()
    |> Ecto.Multi.update(:user, User.password_changeset(user, attrs))
    |> Ecto.Multi.delete_all(:tokens, UserToken.by_user_and_contexts_query(user, :all))
    |> Repo.transaction()
    |> case do
      {:ok, %{user: user}} -> {:ok, user}
      {:error, :user, changeset, _} -> {:error, changeset}
    end
  end

  # Account functions

  def get_account(id) do
    case Repo.get(Account, id) do
      nil -> {:error, Error.not_found(entity: :account)}
      account -> {:ok, account}
    end
  end

  def get_account!(id), do: Repo.get!(Account, id)

  def list_accounts, do: Repo.all(Account)

  def list_accounts_for_user(user_id) do
    Account
    |> Account.where_user_id(user_id)
    |> Repo.all()
  end

  def get_account_for_user(user_id, account_id) do
    account_id
    |> Account.where_id()
    |> Account.where_user_id(user_id)
    |> Repo.one()
    |> case do
      nil -> {:error, Error.not_found(entity: :account, params: %{user_id: user_id, account_id: account_id})}
      account -> {:ok, account}
    end
  end

  def create_account(attrs) do
    Repo.transact(fn ->
      changeset = Account.changeset(%Account{}, attrs)

      case Repo.insert(changeset) do
        {:ok, account} ->
          case ApiTokens.create_for_account(account.id, %{name: "Default"}) do
            {:ok, _api_token} -> {:ok, account}
            error -> error
          end

        {:error, changeset} ->
          {:error, changeset}
      end
    end)
  end

  def update_account(%Account{} = account, attrs) do
    account
    |> Account.changeset(attrs)
    |> Repo.update()
  end

  def delete_account(%Account{} = account) do
    affected_users =
      account.id
      |> list_users_for_account()
      |> Repo.preload([:accounts_users, :accounts])

    if Enum.any?(affected_users, &(length(&1.accounts) == 1)) do
      {:error, Error.invariant(message: "Cannot delete the only account for a user")}
    else
      Repo.delete(account)
    end
  end

  def delete_account_and_account_resources(%Account{} = account, opts \\ []) do
    Repo.transact(fn ->
      if opts[:delete_users] do
        # Delete associated users
        account.id
        |> list_users_for_account()
        |> Enum.each(&delete_user/1)
      end

      # Delete associated API keys
      account.id
      |> ApiTokens.list_tokens_for_account()
      |> Enum.each(&ApiTokens.delete_token_for_account(&1.id, account.id))

      # Delete associated HTTP push and pull consumers
      account.id
      |> Consumers.list_consumers_for_account()
      |> Enum.each(&Consumers.delete_consumer_with_lifecycle/1)

      # Delete associated HTTP endpoints
      account.id
      |> Consumers.list_http_endpoints_for_account()
      |> Enum.each(&Consumers.delete_http_endpoint/1)

      # Delete associated PostgresDatabases
      account.id
      |> Databases.list_dbs_for_account(:replication_slot)
      |> Enum.each(&Databases.delete_db_with_replication_slot/1)

      # Delete associated PostgresReplicationSlots
      account.id
      |> Replication.list_pg_replications_for_account()
      |> Enum.each(&Replication.delete_pg_replication_with_lifecycle/1)

      # Finally, delete the account
      delete_account(account)
    end)
  end

  # AccountUser functions

  @doc """
  Associates a user with an account.
  """
  def associate_user_with_account(%User{} = user, %Account{} = account) do
    with {:ok, account} <- get_account(account.id) do
      %AccountUser{user_id: user.id, account_id: account.id}
      |> AccountUser.changeset(%{current: false})
      |> Repo.insert()
    end
  end

  def set_current_account_for_user(user_id, account_id) do
    # First, check if the user has access to the account
    case Repo.get_by(AccountUser, user_id: user_id, account_id: account_id) do
      nil ->
        {:error, Error.not_found(entity: :account_user)}

      account_user ->
        Repo.transact(fn ->
          # Clear current flag for all user's accounts
          Repo.update_all(
            from(au in AccountUser, where: au.user_id == ^user_id),
            set: [current: false]
          )

          # Set current flag for the selected account
          account_user
          |> Ecto.Changeset.change(%{current: true})
          |> Repo.update!()

          # Fetch and return the updated user with accounts_users preloaded
          user = get_user_with_preloads!(user_id)
          {:ok, user}
        end)
    end
  end

  @doc """
  Removes a user from an account.
  """
  def remove_user_from_account(%User{} = user, %Account{} = account) do
    case Repo.get_by(AccountUser, user_id: user.id, account_id: account.id) do
      nil ->
        {:error, Error.not_found(entity: :account_user)}

      account_user ->
        Repo.delete(account_user)
    end
  end

  # User functions

  @doc """
  Returns the list of users for a given account.
  """
  def list_users_for_account(account_id) do
    Repo.all(User.where_account_id(account_id))
  end

  def list_users do
    Repo.all(User)
  end

  @doc """
  Gets a single user.
  """
  def get_user(id) do
    case Repo.get(User, id) do
      nil -> {:error, Error.not_found(entity: :user)}
      user -> {:ok, user}
    end
  end

  @doc """
  Updates a user.
  """
  def update_user(%User{} = user, attrs) do
    user
    |> User.update_changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Deletes a user.
  """
  def delete_user(%User{} = user) do
    Repo.delete(user)
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking user changes.
  """
  def change_user(%User{} = user, attrs \\ %{}) do
    User.update_changeset(user, attrs)
  end

  @doc """
  Updates a user's GitHub profile information.
  """
  def update_user_github_profile(%User{} = user, attrs) do
    user
    |> User.github_update_changeset(attrs)
    |> Repo.update()
  end

  def list_allocated_bastion_ports_for_account(account_id) do
    account_id
    |> AllocatedBastionPort.where_account_id()
    |> Repo.all()
  end

  def get_or_allocate_bastion_port_for_account(account_id, name) do
    case list_allocated_bastion_ports_for_account(account_id) do
      [] ->
        abp =
          %AllocatedBastionPort{account_id: account_id}
          |> AllocatedBastionPort.create_changeset(%{name: name})
          |> Repo.insert!()

        {:ok, abp}

      [abp | _rest] ->
        abp = abp |> AllocatedBastionPort.update_changeset(%{name: name}) |> Repo.update!()
        {:ok, abp}
    end
  end
end
