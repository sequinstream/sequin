defmodule Sequin.DatabasesRuntime.RecordHandler do
  @moduledoc false
  @behaviour Sequin.DatabasesRuntime.RecordHandlerBehaviour

  alias Sequin.Consumers
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Databases.PostgresDatabase.Table
  alias Sequin.DatabasesRuntime.RecordHandlerBehaviour
  # alias Sequin.Tracer.Server, as: TracerServer

  require Logger

  defmodule Context do
    @moduledoc false
    use TypedStruct

    alias Sequin.Consumers
    alias Sequin.Databases.PostgresDatabase.Table

    typedstruct do
      field :consumer, Consumers.consumer()
      field :table, Table.t()
    end
  end

  @impl RecordHandlerBehaviour
  def init(opts) do
    consumer = Keyword.fetch!(opts, :consumer)
    table = Keyword.fetch!(opts, :table)

    %Context{consumer: consumer, table: table}
  end

  @impl RecordHandlerBehaviour
  def handle_records(%Context{} = ctx, records) do
    %{consumer: consumer} = ctx
    Logger.info("[RecordHandler] Handling #{length(records)} record(s)")

    consumer_records =
      records
      |> Enum.filter(&Consumers.matches_record?(consumer, ctx.table.oid, &1))
      |> Enum.map(&to_consumer_record(ctx, &1))

    Consumers.insert_consumer_records(consumer_records)
    # TODO
    # TracerServer.records_ingested(consumer, consumer_records)
  end

  defp to_consumer_record(%Context{} = ctx, map) do
    %{consumer: consumer, table: table} = ctx

    Sequin.Map.from_ecto(%ConsumerRecord{
      consumer_id: consumer.id,
      table_oid: table.oid,
      record_pks: record_pks(table, map)
    })
  end

  defp record_pks(%Table{} = table, map) do
    table.columns
    |> Enum.filter(& &1.is_pk?)
    |> Enum.sort_by(& &1.attnum)
    |> Enum.map(&Map.fetch!(map, &1.name))
  end
end
