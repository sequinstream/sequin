defmodule Sequin.Repo.Migrations.AddBatchSizeToHttpPushConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env!(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    alter table(:http_push_consumers, prefix: @config_schema) do
      add :batch_size, :integer
    end

    execute "update #{@config_schema}.http_push_consumers set batch_size = 1"

    alter table(:http_push_consumers, prefix: @config_schema) do
      modify :batch_size, :integer, null: false
    end
  end

  def down do
    alter table(:http_push_consumers, prefix: @config_schema) do
      remove :batch_size
    end
  end
end
