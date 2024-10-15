defmodule Sequin.Repo.Migrations.MakeGroupIdNotNullable do
  use Ecto.Migration

  @stream_schema Application.compile_env!(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def up do
    execute "update #{@stream_schema}.consumer_records set group_id = array_to_string(record_pks, ',') where group_id is null"

    alter table(:consumer_records, prefix: @stream_schema) do
      modify :group_id, :string, null: false
    end
  end

  def down do
    alter table(:consumer_records, prefix: @stream_schema) do
      modify :group_id, :string, null: true
    end
  end
end
