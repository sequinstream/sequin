defmodule Sequin.Repo.Migrations.AddHashedTokenToUserTokens do
  use Ecto.Migration
  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:users_tokens, prefix: @config_schema) do
      # `token` is a binary column that depends on the `context` of the user_token.
      # For `accept-team-invite` tokens, `token` is the encrypted token. We need to store
      # the hashed version of the token in the database so we can validate it later.
      add :hashed_token, :binary
    end
  end
end
