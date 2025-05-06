# This file draws heavily from https://github.com/supabase/realtime/blob/main/lib/extensions/postgres/adapters/postgres/decoder/decoder.ex
# which in turns draws from https://github.com/cainophile/pgoutput_decoder
# License: https://github.com/cainophile/pgoutput_decoder/blob/master/LICENSE

defmodule Sequin.Runtime.PostgresAdapter.Decoder do
  @moduledoc """
  Functions for decoding different types of logical replication messages.
  """

  alias Sequin.Runtime.PostgresAdapter.OidDatabase

  require Logger

  defmodule Messages do
    @moduledoc """
    Different types of logical replication messages from Postgres
    """
    use TypedStruct

    defmodule Begin do
      @moduledoc """
      Struct representing the BEGIN message in PostgreSQL's logical decoding output.

      * `final_lsn` - The LSN of the commit that this transaction ended at.
      * `commit_timestamp` - The timestamp of the commit that this transaction ended at.
      * `xid` - The transaction ID of this transaction.
      """
      use TypedStruct

      typedstruct do
        @typedoc "BEGIN message in PostgreSQL's logical decoding output"
        field :final_lsn, {non_neg_integer(), non_neg_integer()}
        field :commit_timestamp, DateTime.t()
        field :xid, non_neg_integer()
      end
    end

    defmodule Commit do
      @moduledoc """
      Struct representing the COMMIT message in PostgreSQL's logical decoding output.

      * `flags` - Bitmask of flags associated with this commit.
      * `lsn` - The LSN of the commit.
      * `end_lsn` - The LSN of the next record in the WAL stream.
      * `commit_timestamp` - The timestamp of the commit.
      """
      use TypedStruct

      typedstruct do
        @typedoc "COMMIT message in PostgreSQL's logical decoding output"
        field :flags, list(), default: []
        field :lsn, {non_neg_integer(), non_neg_integer()}
        field :end_lsn, {non_neg_integer(), non_neg_integer()}
        field :commit_timestamp, DateTime.t()
      end
    end

    defmodule Origin do
      @moduledoc """
      Struct representing the ORIGIN message in PostgreSQL's logical decoding output.

      * `origin_commit_lsn` - The LSN of the commit in the database that the change originated from.
      * `name` - The name of the origin.
      """
      use TypedStruct

      typedstruct do
        @typedoc "ORIGIN message in PostgreSQL's logical decoding output"
        field :origin_commit_lsn, {non_neg_integer(), non_neg_integer()}
        field :name, String.t()
      end
    end

    defmodule Relation.Column do
      @moduledoc """
      Struct representing a column in a relation.

      * `flags` - Bitmask of flags associated with this column.
      * `name` - The name of the column.
      * `type` - The OID of the data type of the column.
      * `pk?` - Whether the column is a primary key. Added post decoder when handling the relation message.
      * `type_modifier` - The type modifier of the column.
      * `attnum` - The attribute number (column position) in the table. Added post decoder when handling the relation message.
      """
      use TypedStruct

      typedstruct do
        @typedoc "Column in a relation"
        field :flags, list(atom()), default: []
        field :name, String.t()
        field :type, String.t()
        field :pk?, boolean()
        field :type_modifier, integer()
        field :attnum, integer()
      end
    end

    defmodule Relation do
      @moduledoc """
      Struct representing the RELATION message in PostgreSQL's logical decoding output.

      * `id` - The OID of the relation.
      * `namespace` - The OID of the namespace that the relation belongs to.
      * `name` - The name of the relation.
      * `replica_identity` - The replica identity setting of the relation.
      * `columns` - A list of columns in the relation.
      """
      use TypedStruct

      typedstruct do
        @typedoc "RELATION message in PostgreSQL's logical decoding output"
        field :id, non_neg_integer()
        field :namespace, String.t()
        field :name, String.t()
        field :replica_identity, atom()
        field :columns, list(Relation.Column.t())
      end
    end

    defmodule Insert do
      @moduledoc """
      Struct representing the INSERT message in PostgreSQL's logical decoding output.

      * `relation_id` - The OID of the relation that the tuple was inserted into.
      * `tuple_data` - The data of the inserted tuple.
      """
      use TypedStruct

      typedstruct do
        @typedoc "INSERT message in PostgreSQL's logical decoding output"
        field :relation_id, non_neg_integer()
        field :tuple_data, {String.t(), String.t()}
      end
    end

    defmodule Update do
      @moduledoc """
      Struct representing the UPDATE message in PostgreSQL's logical decoding output.

      * `relation_id` - The OID of the relation that the tuple was updated in.
      * `changed_key_tuple_data` - The data of the tuple with the old key values.
      * `old_tuple_data` - The data of the tuple before the update.
      * `tuple_data` - The data of the tuple after the update.
      """
      use TypedStruct

      typedstruct do
        @typedoc "UPDATE message in PostgreSQL's logical decoding output"
        field :relation_id, non_neg_integer()
        field :changed_key_tuple_data, {String.t(), String.t()}
        field :old_tuple_data, {String.t(), String.t()}
        field :tuple_data, {String.t(), String.t()}
      end
    end

    defmodule Delete do
      @moduledoc """
      Struct representing the DELETE message in PostgreSQL's logical decoding output.

      * `relation_id` - The OID of the relation that the tuple was deleted from.
      * `changed_key_tuple_data` - The data of the tuple with the old key values.
      * `old_tuple_data` - The data of the tuple before the delete.
      """
      use TypedStruct

      typedstruct do
        @typedoc "DELETE message in PostgreSQL's logical decoding output"
        field :relation_id, non_neg_integer()
        field :changed_key_tuple_data, {String.t(), String.t()}
        field :old_tuple_data, {String.t(), String.t()}
      end
    end

    defmodule Truncate do
      @moduledoc """
      Struct representing the TRUNCATE message in PostgreSQL's logical decoding output.

      * `number_of_relations` - The number of truncated relations.
      * `options` - Additional options provided when truncating the relations.
      * `truncated_relations` - List of relations that have been truncated.
      """
      use TypedStruct

      typedstruct do
        @typedoc "TRUNCATE message in PostgreSQL's logical decoding output"
        field :number_of_relations, non_neg_integer()
        field :options, list(atom())
        field :truncated_relations, list({non_neg_integer(), non_neg_integer()})
      end
    end

    defmodule Type do
      @moduledoc """
      Struct representing the TYPE message in PostgreSQL's logical decoding output.

      * `id` - The OID of the type.
      * `namespace` - The namespace of the type.
      * `name` - The name of the type.
      """
      use TypedStruct

      typedstruct do
        @typedoc "TYPE message in PostgreSQL's logical decoding output"
        field :id, non_neg_integer()
        field :namespace, String.t()
        field :name, String.t()
      end
    end

    defmodule Unsupported do
      @moduledoc """
      Struct representing an unsupported message in PostgreSQL's logical decoding output.

      * `data` - The raw data of the unsupported message.
      """
      use TypedStruct

      typedstruct do
        @typedoc "Unsupported message in PostgreSQL's logical decoding output"
        field :data, String.t()
      end
    end

    defmodule LogicalMessage do
      @moduledoc """
      Struct representing a logical message emitted via pg_logical_emit_message() in PostgreSQL's logical decoding output.

      * `transactional` - Boolean indicating if the message is transactional
      * `prefix` - The prefix/tag of the message
      * `content` - The content of the message
      * `lsn` - The LSN where the message was written
      """
      use TypedStruct

      typedstruct do
        @typedoc "Logical message emitted via pg_logical_emit_message() in PostgreSQL's logical decoding output"
        field :transactional, boolean()
        field :prefix, String.t()
        field :content, String.t()
        field :lsn, {non_neg_integer(), non_neg_integer()}
      end
    end
  end

  @pg_epoch DateTime.from_iso8601("2000-01-01T00:00:00Z")
  @type message ::
          Messages.Relation.t()
          | Messages.Commit.t()
          | Messages.Origin.t()
          | Messages.Insert.t()
          | Messages.Update.t()
          | Messages.Delete.t()
          | Messages.Truncate.t()
          | Messages.Type.t()
          | Messages.LogicalMessage.t()
          | Messages.Unsupported.t()

  @doc """
  Parses logical replication messages from Postgres

  ## Examples

      iex> decode_message(<<73, 0, 0, 96, 0, 78, 0, 2, 116, 0, 0, 0, 3, 98, 97, 122, 116, 0, 0, 0, 3, 53, 54, 48>>)
      %Realtime.Adapters.Postgres.Decoder.Messages.Insert{relation_id: 24576, tuple_data: {"baz", "560"}}

  """
  def decode_message(message) when is_binary(message) do
    # Logger.debug("Message before conversion " <> message)
    decode_message_impl(message)
  end

  defp decode_message_impl(<<"B", lsn::binary-8, timestamp::integer-64, xid::integer-32>>) do
    %Messages.Begin{
      final_lsn: decode_lsn(lsn),
      commit_timestamp: pgtimestamp_to_timestamp(timestamp),
      xid: xid
    }
  end

  defp decode_message_impl(<<"C", _flags::binary-1, lsn::binary-8, end_lsn::binary-8, timestamp::integer-64>>) do
    %Messages.Commit{
      flags: [],
      lsn: decode_lsn(lsn),
      end_lsn: decode_lsn(end_lsn),
      commit_timestamp: pgtimestamp_to_timestamp(timestamp)
    }
  end

  # TODO: Verify this is correct with real data from Postgres
  defp decode_message_impl(<<"O", lsn::binary-8, name::binary>>) do
    %Messages.Origin{
      origin_commit_lsn: decode_lsn(lsn),
      name: name
    }
  end

  defp decode_message_impl(<<"R", id::integer-32, rest::binary>>) do
    [
      namespace
      | [name | [<<replica_identity::binary-1, _number_of_columns::integer-16, columns::binary>>]]
    ] = String.split(rest, <<0>>, parts: 3)

    # TODO: Handle case where pg_catalog is blank, we should still return the schema as pg_catalog
    friendly_replica_identity =
      case replica_identity do
        "d" -> :default
        "n" -> :nothing
        "f" -> :all_columns
        "i" -> :index
      end

    %Messages.Relation{
      id: id,
      namespace: namespace,
      name: name,
      replica_identity: friendly_replica_identity,
      columns: decode_columns(columns)
    }
  end

  defp decode_message_impl(<<"I", relation_id::integer-32, "N", number_of_columns::integer-16, tuple_data::binary>>) do
    {<<>>, decoded_tuple_data} = decode_tuple_data(tuple_data, number_of_columns)

    %Messages.Insert{
      relation_id: relation_id,
      tuple_data: decoded_tuple_data
    }
  end

  defp decode_message_impl(<<"U", relation_id::integer-32, "N", number_of_columns::integer-16, tuple_data::binary>>) do
    {<<>>, decoded_tuple_data} = decode_tuple_data(tuple_data, number_of_columns)

    %Messages.Update{
      relation_id: relation_id,
      tuple_data: decoded_tuple_data
    }
  end

  defp decode_message_impl(
         <<"U", relation_id::integer-32, key_or_old::binary-1, number_of_columns::integer-16, tuple_data::binary>>
       )
       when key_or_old == "O" or key_or_old == "K" do
    {<<"N", new_number_of_columns::integer-16, new_tuple_binary::binary>>, old_decoded_tuple_data} =
      decode_tuple_data(tuple_data, number_of_columns)

    {<<>>, decoded_tuple_data} = decode_tuple_data(new_tuple_binary, new_number_of_columns)

    base_update_msg = %Messages.Update{
      relation_id: relation_id,
      tuple_data: decoded_tuple_data
    }

    case key_or_old do
      "K" -> Map.put(base_update_msg, :changed_key_tuple_data, old_decoded_tuple_data)
      "O" -> Map.put(base_update_msg, :old_tuple_data, old_decoded_tuple_data)
    end
  end

  defp decode_message_impl(
         <<"D", relation_id::integer-32, key_or_old::binary-1, number_of_columns::integer-16, tuple_data::binary>>
       )
       when key_or_old == "K" or key_or_old == "O" do
    {<<>>, decoded_tuple_data} = decode_tuple_data(tuple_data, number_of_columns)

    base_delete_msg = %Messages.Delete{
      relation_id: relation_id
    }

    case key_or_old do
      "K" -> Map.put(base_delete_msg, :changed_key_tuple_data, decoded_tuple_data)
      "O" -> Map.put(base_delete_msg, :old_tuple_data, decoded_tuple_data)
    end
  end

  defp decode_message_impl(<<"T", number_of_relations::integer-32, options::integer-8, column_ids::binary>>) do
    truncated_relations =
      for relation_id_bin <- column_ids |> :binary.bin_to_list() |> Enum.chunk_every(4),
          do: relation_id_bin |> :binary.list_to_bin() |> :binary.decode_unsigned()

    decoded_options =
      case options do
        0 -> []
        1 -> [:cascade]
        2 -> [:restart_identity]
        3 -> [:cascade, :restart_identity]
      end

    %Messages.Truncate{
      number_of_relations: number_of_relations,
      options: decoded_options,
      truncated_relations: truncated_relations
    }
  end

  defp decode_message_impl(<<"Y", data_type_id::integer-32, namespace_and_name::binary>>) do
    [namespace, name_with_null] = :binary.split(namespace_and_name, <<0>>)
    name = String.slice(name_with_null, 0..-2//1)

    %Messages.Type{
      id: data_type_id,
      namespace: namespace,
      name: name
    }
  end

  defp decode_message_impl(<<"M", transactional::binary-1, lsn::binary-8, rest::binary>>) do
    [prefix, <<_length::integer-32, content::binary>>] = String.split(rest, <<0>>, parts: 2)

    %Messages.LogicalMessage{
      transactional: transactional == "t",
      prefix: prefix,
      content: content,
      lsn: decode_lsn(lsn)
    }
  end

  defp decode_message_impl(binary), do: %Messages.Unsupported{data: binary}

  defp decode_tuple_data(binary, columns_remaining, accumulator \\ [])

  defp decode_tuple_data(remaining_binary, 0, accumulator) when is_binary(remaining_binary),
    do: {remaining_binary, accumulator |> Enum.reverse() |> List.to_tuple()}

  defp decode_tuple_data(<<"n", rest::binary>>, columns_remaining, accumulator) do
    decode_tuple_data(rest, columns_remaining - 1, [nil | accumulator])
  end

  defp decode_tuple_data(<<"u", rest::binary>>, columns_remaining, accumulator) do
    decode_tuple_data(rest, columns_remaining - 1, [:unchanged_toast | accumulator])
  end

  defp decode_tuple_data(<<"t", column_length::integer-32, rest::binary>>, columns_remaining, accumulator) do
    value = :erlang.binary_part(rest, {0, column_length})

    decode_tuple_data(
      :erlang.binary_part(rest, {byte_size(rest), -(byte_size(rest) - column_length)}),
      columns_remaining - 1,
      [value | accumulator]
    )
  end

  defp decode_columns(binary, accumulator \\ [])
  defp decode_columns(<<>>, accumulator), do: Enum.reverse(accumulator)

  defp decode_columns(<<flags::integer-8, rest::binary>>, accumulator) do
    [name | [<<data_type_id::integer-32, type_modifier::integer-32, columns::binary>>]] =
      String.split(rest, <<0>>, parts: 2)

    decoded_flags =
      case flags do
        1 -> [:key]
        _ -> []
      end

    decode_columns(columns, [
      %Messages.Relation.Column{
        name: name,
        flags: decoded_flags,
        type: OidDatabase.name_for_type_id(data_type_id),
        # type: data_type_id,
        type_modifier: type_modifier
      }
      | accumulator
    ])
  end

  defp pgtimestamp_to_timestamp(microsecond_offset) when is_integer(microsecond_offset) do
    {:ok, epoch, 0} = @pg_epoch

    DateTime.add(epoch, microsecond_offset, :microsecond)
  end

  defp decode_lsn(<<xlog_file::integer-32, xlog_offset::integer-32>>), do: {xlog_file, xlog_offset}

  def extract_relation_id(<<"I", relation_id::integer-32, _rest::binary>>), do: {:ok, relation_id}
  def extract_relation_id(<<"U", relation_id::integer-32, _rest::binary>>), do: {:ok, relation_id}
  def extract_relation_id(<<"D", relation_id::integer-32, _rest::binary>>), do: {:ok, relation_id}
  def extract_relation_id(<<"R", relation_id::integer-32, _rest::binary>>), do: {:ok, relation_id}
  # truncate?
  def extract_relation_id(_), do: :no_relation_id
end
