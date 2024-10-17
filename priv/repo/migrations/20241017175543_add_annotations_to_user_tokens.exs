defmodule Sequin.Repo.Migrations.AddAnnotationsToUserTokens do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:users_tokens, prefix: @config_schema) do
      # Annotations are used to store additional information about the token.
      # Very similar to Kubernetes annotations, or Stripe metadata.
      # This is useful for storing information about the token that is not
      # directly relevant to the token itself.
      add :annotations, :map
    end
  end
end
