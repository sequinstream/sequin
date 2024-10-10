defmodule Sequin.Repo.Migrations.CreateUsersAccountsAndModifyUsers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    # 1. Create the accounts_users join table
    create table(:accounts_users, prefix: @config_schema) do
      add :user_id, references(:users, on_delete: :delete_all, prefix: @config_schema),
        null: false

      add :account_id, references(:accounts, on_delete: :delete_all, prefix: @config_schema),
        null: false

      add :current, :boolean, default: false, null: false

      timestamps()
    end

    create unique_index(:accounts_users, [:user_id, :account_id], prefix: @config_schema)

    create unique_index(:accounts_users, [:user_id, :current],
             prefix: @config_schema,
             where: "current = true",
             name: "idx_users_current"
           )

    # 2. Create a function to migrate data and call it
    execute """
    INSERT INTO #{@config_schema}.accounts_users (user_id, account_id, current, inserted_at, updated_at)
    SELECT id, account_id, true, inserted_at, NOW()
    FROM #{@config_schema}.users
    WHERE account_id IS NOT NULL
    """

    # 3. Create a constraint to ensure at least one accounts_users row for a user
    execute """
    CREATE OR REPLACE FUNCTION #{@config_schema}.ensure_user_has_account()
    RETURNS TRIGGER AS $$
    BEGIN
      IF EXISTS (SELECT 1 FROM #{@config_schema}.users WHERE id = OLD.user_id) AND
         (SELECT COUNT(*) FROM #{@config_schema}.accounts_users WHERE user_id = OLD.user_id) = 1 THEN
        RAISE EXCEPTION 'User must have at least one associated account';
      END IF;
      RETURN OLD;
    END;
    $$ LANGUAGE plpgsql;
    """

    execute """
    CREATE TRIGGER ensure_user_has_account_trigger
    BEFORE DELETE ON #{@config_schema}.accounts_users
    FOR EACH ROW
    EXECUTE FUNCTION #{@config_schema}.ensure_user_has_account();
    """

    # 4. Add trigger to prevent updates to account_id and user_id
    execute """
    CREATE OR REPLACE FUNCTION #{@config_schema}.prevent_account_user_update()
    RETURNS TRIGGER AS $$
    BEGIN
      IF OLD.account_id != NEW.account_id OR OLD.user_id != NEW.user_id THEN
        RAISE EXCEPTION 'For security, you cannot change account_id or user_id in accounts_users table.';
      END IF;
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """

    execute """
    CREATE TRIGGER prevent_account_user_update_trigger
    BEFORE UPDATE ON #{@config_schema}.accounts_users
    FOR EACH ROW
    EXECUTE FUNCTION #{@config_schema}.prevent_account_user_update();
    """

    # 5. Remove the NOT NULL constraint on user.account_id
    alter table(:users, prefix: @config_schema) do
      modify :account_id, :uuid, null: true
    end
  end

  def down do
    # Revert the changes in reverse order

    # 6. Update null account_id values in users table
    execute """
    UPDATE #{@config_schema}.users u
    SET account_id = COALESCE(
      (SELECT account_id FROM #{@config_schema}.accounts_users au
       WHERE au.user_id = u.id AND au.current = true
       LIMIT 1),
      (SELECT account_id FROM #{@config_schema}.accounts_users au
       WHERE au.user_id = u.id
       ORDER BY au.inserted_at ASC
       LIMIT 1)
    )
    WHERE u.account_id IS NULL;
    """

    # 5. Add the NOT NULL constraint back to user.account_id
    alter table(:users, prefix: @config_schema) do
      modify :account_id, :uuid, null: false
    end

    # 4. Remove the trigger and function for preventing updates
    execute "DROP TRIGGER IF EXISTS prevent_account_user_update_trigger ON #{@config_schema}.accounts_users;"
    execute "DROP FUNCTION IF EXISTS #{@config_schema}.prevent_account_user_update() CASCADE;"

    # 3. Remove the trigger and function
    execute "DROP FUNCTION IF EXISTS #{@config_schema}.ensure_user_has_account() CASCADE;"
    execute "DROP TRIGGER IF EXISTS ensure_user_has_account_trigger ON #{@config_schema}.users;"

    # 2. No need to revert the data migration, as the join table will be dropped

    # 1. Drop the accounts_users table
    drop table(:accounts_users, prefix: @config_schema)
  end
end
