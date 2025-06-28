defmodule Sequin.Runtime.SlotProducer.Relation do
  @moduledoc """
  Represents a relation in the database.
  """
  use TypedStruct

  alias Sequin.Databases.DatabaseUpdateWorker
  alias Sequin.Postgres
  alias Sequin.Runtime.PostgresAdapter.Decoder
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Relation, as: RawRelation
  alias Sequin.Runtime.SlotProducer.PostgresRelationHashCache

  require Logger

  typedstruct do
    field :id, non_neg_integer()
    field :columns, list(map())
    field :schema, String.t()
    field :table, String.t()
    field :parent_table_id, integer()
  end

  defmodule Column do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :attnum, non_neg_integer()
      field :type_modifier, non_neg_integer()
      field :pk?, boolean()
      field :type, String.t()
      field :name, String.t()
      field :flags, list(String.t())
    end
  end

  @spec parse_relation(binary(), db_id :: String.t(), Postgres.db_conn()) :: {:ok, map()} | {:error, String.t()}
  def parse_relation(msg, db_id, conn) do
    %RawRelation{id: id, columns: columns, namespace: schema, name: table} = raw_relation = Decoder.decode_message(msg)

    columns =
      Enum.map(columns, fn %RawRelation.Column{} = col ->
        %Column{
          attnum: col.attnum,
          type_modifier: col.type_modifier,
          pk?: col.pk?,
          type: col.type,
          name: col.name,
          flags: col.flags
        }
      end)

    # First, determine if this is a partition and get its parent table info
    partition_query = """
    SELECT
      p.inhparent as parent_id,
      pn.nspname as parent_schema,
      pc.relname as parent_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_inherits p ON p.inhrelid = c.oid
    JOIN pg_class pc ON pc.oid = p.inhparent
    JOIN pg_namespace pn ON pn.oid = pc.relnamespace
    WHERE c.oid = $1;
    """

    parent_info =
      case Postgres.query(conn, partition_query, [id]) do
        {:ok, %{rows: [[parent_id, parent_schema, parent_name]]}} ->
          # It's a partition, use the parent info
          %{id: parent_id, schema: parent_schema, name: parent_name}

        {:ok, %{rows: []}} ->
          # Not a partition, use its own info
          %{id: id, schema: schema, name: table}
      end

    # Get attnums for the actual table
    attnum_query = """
    with pk_columns as (
      select a.attname
      from pg_index i
      join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
      where i.indrelid = $1
      and i.indisprimary
    ),
    column_info as (
      select a.attname, a.attnum,
             (select typname
              from pg_type
              where oid = case when t.typtype = 'd'
                              then t.typbasetype
                              else t.oid
                         end) as base_type
      from pg_attribute a
      join pg_type t on t.oid = a.atttypid
      where a.attrelid = $1
      and a.attnum > 0
      and not a.attisdropped
    )
    select c.attname, c.attnum, c.base_type, (pk.attname is not null) as is_pk
    from column_info c
    left join pk_columns pk on pk.attname = c.attname;
    """

    {:ok, %{rows: attnum_rows}} = Postgres.query(conn, attnum_query, [parent_info.id])

    # Enrich columns with primary key information and attnums
    enriched_columns =
      Enum.map(columns, fn %{name: name} = column ->
        case Enum.find(attnum_rows, fn [col_name, _, _, _] -> col_name == name end) do
          [_, attnum, base_type, is_pk] ->
            %Column{column | pk?: is_pk, attnum: attnum, type: base_type}

          nil ->
            column
        end
      end)

    # Create a relation with enriched columns
    enriched_relation = %{raw_relation | columns: enriched_columns}

    # Compare schema hashes to detect changes
    current_hash = PostgresRelationHashCache.compute_schema_hash(enriched_relation)
    stored_hash = PostgresRelationHashCache.get_schema_hash(db_id, id)

    unless stored_hash == current_hash do
      Logger.info("[SlotProcessorServer] Schema changes detected for table, enqueueing database update",
        relation_id: id,
        schema: parent_info.schema,
        table: parent_info.name
      )

      PostgresRelationHashCache.update_schema_hash(db_id, id, current_hash)
      DatabaseUpdateWorker.enqueue(db_id, unique_period: 0)
    end

    %__MODULE__{
      id: id,
      columns: enriched_columns,
      schema: parent_info.schema,
      table: parent_info.name,
      parent_table_id: parent_info.id
    }
  end
end
