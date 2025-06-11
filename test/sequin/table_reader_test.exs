defmodule Sequin.TableReaderTest do
  use Sequin.DataCase, async: true

  alias Sequin.Databases
  alias Sequin.Databases.ConnectionCache
  alias Sequin.Factory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.Runtime.KeysetCursor
  alias Sequin.Runtime.TableReader
  alias Sequin.TestSupport.Models.CharacterMultiPK

  setup do
    db = DatabasesFactory.insert_configured_postgres_database!(tables: [])
    ConnectionCache.cache_connection(db, Repo)

    {:ok, tables} = Databases.tables(db)

    characters_table = Enum.find(tables, &(&1.name == "Characters"))
    characters_multi_pk_table = Enum.find(tables, &(&1.name == "characters_multi_pk"))
    characters_detailed_table = Enum.find(tables, &(&1.name == "characters_detailed"))

    character_col_attnums = map_column_attnums(characters_table)
    character_multi_pk_attnums = map_column_attnums(characters_multi_pk_table)
    character_detailed_attnums = map_column_attnums(characters_detailed_table)

    # Set sort_column_attnum for both tables
    characters_table = %{characters_table | sort_column_attnum: character_col_attnums["updated_at"]}

    characters_multi_pk_table = %{
      characters_multi_pk_table
      | sort_column_attnum: character_multi_pk_attnums["updated_at"]
    }

    characters_detailed_table = %{
      characters_detailed_table
      | sort_column_attnum: character_detailed_attnums["updated_at"]
    }

    ConnectionCache.cache_connection(db, Repo)

    character_consumer =
      Repo.preload(
        ConsumersFactory.insert_sink_consumer!(
          account_id: db.account_id,
          postgres_database_id: db.id,
          source: ConsumersFactory.source_attrs(include_table_oids: [characters_table.oid])
        ),
        [:postgres_database, :filter]
      )

    backfill =
      ConsumersFactory.insert_active_backfill!(
        account_id: db.account_id,
        sink_consumer_id: character_consumer.id,
        initial_min_cursor: %{characters_table.sort_column_attnum => NaiveDateTime.utc_now()},
        sort_column_attnum: characters_table.sort_column_attnum,
        table_oid: characters_table.oid
      )

    character_multi_pk_consumer =
      Repo.preload(
        ConsumersFactory.insert_sink_consumer!(
          account_id: db.account_id,
          postgres_database_id: db.id,
          source: ConsumersFactory.source_attrs(include_table_oids: [characters_multi_pk_table.oid])
        ),
        [:postgres_database, :filter]
      )

    character_detailed_consumer =
      Repo.preload(
        ConsumersFactory.insert_sink_consumer!(
          account_id: db.account_id,
          postgres_database_id: db.id
        ),
        [:postgres_database, :filter]
      )

    ConsumersFactory.insert_active_backfill!(
      account_id: db.account_id,
      sink_consumer_id: character_detailed_consumer.id,
      initial_min_cursor: %{characters_table.sort_column_attnum => NaiveDateTime.utc_now()},
      sort_column_attnum: characters_table.sort_column_attnum,
      table_oid: characters_table.oid
    )

    %{
      db: db,
      backfill: backfill,
      character_consumer: character_consumer,
      character_multi_pk_consumer: character_multi_pk_consumer,
      character_detailed_consumer: character_detailed_consumer,
      consumer_id: "test_consumer_#{Factory.unique_integer()}",
      tables: tables,
      characters_table: characters_table,
      characters_multi_pk_table: characters_multi_pk_table,
      character_col_attnums: character_col_attnums,
      character_multi_pk_attnums: character_multi_pk_attnums,
      characters_detailed_table: characters_detailed_table
    }
  end

  describe "cursor operations" do
    test "fetch_cursors returns :error when no cursors exist", %{consumer_id: consumer_id} do
      assert :error == TableReader.fetch_cursors(consumer_id)
    end

    test "update_cursor and fetch_cursors work correctly", %{consumer_id: consumer_id} do
      cursor = %{1 => "2023-01-01", 2 => 123}

      assert :ok == TableReader.update_cursor(consumer_id, cursor)

      assert {:ok, %{"min" => ^cursor}} = TableReader.fetch_cursors(consumer_id)
    end

    test "cursor retrieves cursor value", %{consumer_id: consumer_id} do
      cursor = %{1 => "2023-02-01", 2 => 789}

      assert :ok == TableReader.update_cursor(consumer_id, cursor)
      assert ^cursor = TableReader.cursor(consumer_id)
    end

    test "cursor returns nil for non-existent cursor", %{consumer_id: consumer_id} do
      assert nil == TableReader.cursor(consumer_id)
    end

    test "delete_cursor removes cursor", %{consumer_id: consumer_id} do
      cursor = %{1 => "2023-03-01", 2 => 1213}

      assert :ok == TableReader.update_cursor(consumer_id, cursor)
      assert {:ok, _} = TableReader.fetch_cursors(consumer_id)

      assert :ok == TableReader.delete_cursor(consumer_id)
      assert :error == TableReader.fetch_cursors(consumer_id)
    end
  end

  describe "with_watermark/4" do
    test "emits watermark messages around the given function", %{db: db} do
      table_oid = 12_345
      batch_id = "test_batch"

      # Let an actual connection be created
      ConnectionCache.invalidate_connection(db)

      # This does not test that watermark messages are emitted, we will do this in a separate test

      {:ok, result, _lsn} =
        TableReader.with_watermark(db, UUID.uuid4(), UUID.uuid4(), batch_id, table_oid, fn conn ->
          Postgrex.query(conn, "select 1", [])
        end)

      assert result.rows == [[1]]
    end
  end

  describe "fetch_batch/4" do
    test "fetches a batch of records with default limit", %{
      db: db,
      backfill: backfill,
      characters_table: table,
      character_consumer: consumer
    } do
      now = NaiveDateTime.utc_now()
      char1 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -3, :second))
      char2 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -2, :second))
      char3 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -1, :second))

      {:ok, _first_row, initial_cursor} = TableReader.fetch_first_row(db, table)

      {:ok, %{messages: messages, next_cursor: next_cursor}} =
        TableReader.fetch_batch(db, consumer, backfill, table, initial_cursor, include_min: true)

      assert length(messages) == 3

      # Verify the messages contain the correct record data
      assert Enum.at(messages, 0).data.record["id"] == char1.id
      assert Enum.at(messages, 1).data.record["id"] == char2.id
      assert Enum.at(messages, 2).data.record["id"] == char3.id

      # Verify next cursor matches last record
      assert next_cursor[table.sort_column_attnum] == char3.updated_at

      {:ok, %{messages: messages, next_cursor: next_cursor}} =
        TableReader.fetch_batch(db, consumer, backfill, table, initial_cursor, include_min: false)

      assert length(messages) == 2
      assert Enum.at(messages, 0).data.record["id"] == char2.id
      assert Enum.at(messages, 1).data.record["id"] == char3.id

      # Verify next cursor matches last record
      assert next_cursor[table.sort_column_attnum] == char3.updated_at
    end

    test "works with nil sort column", %{
      db: db,
      backfill: backfill,
      characters_table: table,
      character_consumer: consumer
    } do
      # Create a table with nil sort_column_attnum
      table_with_nil_sort = %{table | sort_column_attnum: nil}

      now = NaiveDateTime.utc_now()
      CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -3, :second))
      CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -2, :second))
      CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -1, :second))

      {:ok, _first_row, initial_cursor} = TableReader.fetch_first_row(db, table_with_nil_sort)

      {:ok, %{messages: messages, next_cursor: next_cursor}} =
        TableReader.fetch_batch(db, consumer, backfill, table_with_nil_sort, initial_cursor, include_min: true)

      assert length(messages) == 3

      # Verify next cursor only contains primary key columns
      refute Map.has_key?(next_cursor, table.sort_column_attnum)
    end

    test "correctly handles nil and populated UUID, UUID[] and bytea fields", %{
      db: db,
      backfill: backfill,
      characters_detailed_table: table,
      character_detailed_consumer: consumer
    } do
      # Insert two characters with different combinations of special fields
      char1 =
        CharacterFactory.insert_character_detailed!(
          house_id: nil,
          related_houses: [],
          binary_data: <<1, 2, 3>>,
          status: :active
        )

      char2 =
        CharacterFactory.insert_character_detailed!(
          house_id: UUID.uuid4(),
          related_houses: [UUID.uuid4(), UUID.uuid4()],
          binary_data: <<4, 5, 6>>,
          status: :retired
        )

      {:ok, _first_row, initial_min_cursor} = TableReader.fetch_first_row(db, table)

      {:ok, %{messages: messages, next_cursor: _next_cursor}} =
        TableReader.fetch_batch(
          db,
          consumer,
          backfill,
          table,
          initial_min_cursor,
          limit: 10,
          include_min: true
        )

      assert length(messages) == 2

      # Find the results corresponding to our inserted characters
      record1 = Enum.find(messages, &(&1.data.record["id"] == char1.id)).data.record
      record2 = Enum.find(messages, &(&1.data.record["id"] == char2.id)).data.record

      # Verify enum handling
      assert record1["status"] == "active"
      assert record2["status"] == "retired"

      # Verify UUID handling
      refute record1["house_id"]
      assert {:ok, _} = UUID.info(record2["house_id"])
      assert record2["house_id"] == char2.house_id

      # Verify UUID array handling
      assert record1["related_houses"] == []
      assert length(record2["related_houses"]) == 2
      assert Enum.all?(record2["related_houses"], fn uuid -> match?({:ok, _}, UUID.info(uuid)) end)
      assert record2["related_houses"] == char2.related_houses

      # Verify bytea handling
      assert record1["binary_data"] == "\\x010203"
      assert record2["binary_data"] == "\\x040506"
    end

    test "respects the limit parameter", %{
      db: db,
      backfill: backfill,
      characters_table: table,
      character_consumer: consumer
    } do
      now = NaiveDateTime.utc_now()
      char1 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -2, :second))
      char2 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -1, :second))
      _char3 = CharacterFactory.insert_character!(updated_at: now)

      {:ok, _first_row, initial_cursor} = TableReader.fetch_first_row(db, table)

      {:ok, %{messages: messages, next_cursor: next_cursor}} =
        TableReader.fetch_batch(db, consumer, backfill, table, initial_cursor, limit: 2, include_min: true)

      assert length(messages) == 2
      assert messages |> Enum.at(0) |> Map.get(:data) |> Map.get(:record) |> Map.get("id") == char1.id
      assert messages |> Enum.at(1) |> Map.get(:data) |> Map.get(:record) |> Map.get("id") == char2.id

      # Verify next cursor matches last record in batch
      assert next_cursor[table.sort_column_attnum] == char2.updated_at
    end

    test "handles empty result set", %{
      db: db,
      backfill: backfill,
      characters_table: table,
      character_consumer: consumer
    } do
      cursor = KeysetCursor.min_cursor(table, NaiveDateTime.utc_now())

      {:ok, %{messages: messages, next_cursor: next_cursor}} =
        TableReader.fetch_batch(db, consumer, backfill, table, cursor)

      assert messages == []
      assert next_cursor == nil
    end
  end

  describe "fetch_batch_pks" do
    test "fetches primary keys for single PK table", %{
      db: db,
      characters_table: table
    } do
      now = NaiveDateTime.utc_now()
      char1 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -3, :second))
      char2 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -2, :second))
      char3 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -1, :second))

      {:ok, _first_row, initial_cursor} = TableReader.fetch_first_row(db, table)

      # Fetch primary keys
      {:ok, %{pks: primary_keys, next_cursor: next_cursor}} =
        TableReader.fetch_batch_pks(db, table, initial_cursor, include_min: true)

      assert length(primary_keys) == 3
      assert primary_keys == [[to_string(char1.id)], [to_string(char2.id)], [to_string(char3.id)]]
      # Verify next cursor matches last record
      assert next_cursor[table.sort_column_attnum] == char3.updated_at

      # Test with include_min: false
      {:ok, %{pks: primary_keys, next_cursor: next_cursor}} =
        TableReader.fetch_batch_pks(db, table, initial_cursor, include_min: false)

      assert length(primary_keys) == 2
      assert primary_keys == [[to_string(char2.id)], [to_string(char3.id)]]
      assert next_cursor[table.sort_column_attnum] == char3.updated_at
    end

    test "works with nil sort column", %{
      db: db,
      characters_table: table
    } do
      # Create a table with nil sort_column_attnum
      table_with_nil_sort = %{table | sort_column_attnum: nil}

      CharacterFactory.insert_character!()
      CharacterFactory.insert_character!()
      CharacterFactory.insert_character!()

      {:ok, _first_row, initial_cursor} = TableReader.fetch_first_row(db, table_with_nil_sort)

      # Fetch primary keys
      {:ok, %{pks: primary_keys, next_cursor: next_cursor}} =
        TableReader.fetch_batch_pks(db, table_with_nil_sort, initial_cursor, include_min: true)

      assert length(primary_keys) == 3

      # Verify next cursor only contains primary key columns
      refute Map.has_key?(next_cursor, table.sort_column_attnum)
    end

    test "fetches primary keys for multi PK table", %{
      db: db,
      characters_multi_pk_table: table
    } do
      char1 = CharacterFactory.insert_character_multi_pk!()
      char2 = CharacterFactory.insert_character_multi_pk!()
      char3 = CharacterFactory.insert_character_multi_pk!()

      {:ok, _first_row, initial_cursor} = TableReader.fetch_first_row(db, table)

      # Fetch primary keys
      {:ok, %{pks: primary_keys, next_cursor: next_cursor}} =
        TableReader.fetch_batch_pks(db, table, initial_cursor, include_min: true)

      assert length(primary_keys) == 3
      assert next_cursor != nil

      expected_pks = [
        CharacterMultiPK.record_pks(char1),
        CharacterMultiPK.record_pks(char2),
        CharacterMultiPK.record_pks(char3)
      ]

      assert primary_keys == Enum.map(expected_pks, fn pks -> Enum.map(pks, &to_string/1) end)
    end

    test "correctly paginates primary keys with next_cursor", %{
      db: db,
      characters_table: table
    } do
      now = NaiveDateTime.utc_now()
      char1 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -5, :second))
      char2 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -4, :second))
      char3 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -3, :second))
      char4 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -2, :second))
      char5 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -1, :second))

      {:ok, _first_row, initial_cursor} = TableReader.fetch_first_row(db, table)

      # Fetch first page of primary keys
      {:ok, %{pks: primary_keys, next_cursor: first_page_cursor}} =
        TableReader.fetch_batch_pks(db, table, initial_cursor,
          include_min: true,
          limit: 3
        )

      assert length(primary_keys) == 3
      assert Enum.all?(primary_keys, fn pks -> Enum.all?(pks, &is_binary/1) end)
      assert primary_keys == [[to_string(char1.id)], [to_string(char2.id)], [to_string(char3.id)]]
      assert first_page_cursor[table.sort_column_attnum] == char3.updated_at

      # Fetch second page using next_cursor
      {:ok, %{pks: primary_keys, next_cursor: second_page_cursor}} =
        TableReader.fetch_batch_pks(db, table, first_page_cursor,
          include_min: false,
          limit: 3
        )

      assert length(primary_keys) == 2
      assert primary_keys == [[to_string(char4.id)], [to_string(char5.id)]]
      assert second_page_cursor[table.sort_column_attnum] == char5.updated_at
    end

    test "handles empty result set", %{
      db: db,
      characters_table: table
    } do
      cursor = KeysetCursor.min_cursor(table, NaiveDateTime.utc_now())

      {:ok, %{pks: primary_keys, next_cursor: next_cursor}} = TableReader.fetch_batch_pks(db, table, cursor)

      assert primary_keys == []
      assert next_cursor == nil
    end
  end

  defp map_column_attnums(table) do
    Map.new(table.columns, fn column -> {column.name, column.attnum} end)
  end

  # defp assert_character_equal(result, character) when is_struct(result) do
  #   assert_character_equal(Map.from_struct(result), character)
  # end

  # defp assert_character_equal(result, character) do
  #   assert_maps_equal(
  #     result,
  #     Map.from_struct(character),
  #     [
  #       "id",
  #       "name",
  #       "updated_at"
  #     ],
  #     indifferent_keys: true
  #   )
  # end

  # defp assert_multi_pk_character_equal(result, character) when is_struct(result) do
  #   assert_multi_pk_character_equal(Map.from_struct(result), character)
  # end

  # defp assert_multi_pk_character_equal(result, character) do
  #   assert_maps_equal(
  #     result,
  #     Map.from_struct(character),
  #     [
  #       "id_integer",
  #       "id_string",
  #       "id_uuid",
  #       "name",
  #       "house",
  #       "updated_at"
  #     ],
  #     indifferent_keys: true
  #   )
  # end
end
