defmodule Sequin.Repo.Migrations.AlterHttpPushConsumerToPolymorphicSinkConsumer do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    # Add new destination column
    alter table(:http_push_consumers, prefix: @config_schema) do
      add :destination, :jsonb
      add :type, :string, generated: "always as (destination->>'type') stored"
      modify :http_endpoint_id, :binary_id, null: true
    end

    # Populate the destination column with existing http push config
    execute """
    update #{@config_schema}.http_push_consumers
    set destination = jsonb_build_object(
      'type', 'http_push',
      'http_endpoint_id', http_endpoint_id,
      'http_endpoint_path', http_endpoint_path
    )
    """

    rename(table(:http_push_consumers, prefix: @config_schema),
      to: table(:destination_consumers, prefix: @config_schema)
    )
  end

  def down do
    rename(table(:destination_consumers, prefix: @config_schema),
      to: table(:http_push_consumers, prefix: @config_schema)
    )

    alter table(:http_push_consumers, prefix: @config_schema) do
      remove :type
      remove :destination
      modify :http_endpoint_id, :binary_id, null: false
    end
  end
end
