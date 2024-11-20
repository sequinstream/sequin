defmodule Sequin.Repo.Migrations.RenameSinkConsumerAccountIdNameIndex do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    execute(
      "ALTER INDEX #{@config_schema}.http_push_consumers_account_id_name_index RENAME TO sink_consumers_account_id_name_index",
      "ALTER INDEX #{@config_schema}.sink_consumers_account_id_name_index RENAME TO http_push_consumers_account_id_name_index"
    )
  end
end
