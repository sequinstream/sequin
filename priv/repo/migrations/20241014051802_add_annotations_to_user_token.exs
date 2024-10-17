defmodule Sequin.Repo.Migrations.AddAnnotationsToUserToken do
  use Ecto.Migration

  def change do
    alter table(:users_tokens) do
      # Annotations are used to store additional information about the token.
      # Very similar to Kubernetes annotations, or Stripe metadata.
      # This is useful for storing information about the token that is not
      # directly relevant to the token itself.
      add :annotations, :map
    end
  end
end
