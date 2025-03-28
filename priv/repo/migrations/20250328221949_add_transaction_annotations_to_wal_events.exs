defmodule Sequin.Repo.Migrations.AddTransactionAnnotationsToWalEvents do
  use Ecto.Migration

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    alter table(:wal_events, prefix: @stream_schema) do
      add :transaction_annotations, :jsonb, null: true
    end
  end
end
