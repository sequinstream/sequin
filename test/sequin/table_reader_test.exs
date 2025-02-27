defmodule Sequin.TableReaderTest do
  use Sequin.DataCase, async: true

  alias Sequin.Databases
  alias Sequin.Databases.ConnectionCache
  alias Sequin.DatabasesRuntime.KeysetCursor
  alias Sequin.DatabasesRuntime.TableReader
  alias Sequin.Factory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.DatabasesFactory
  alias Sequin.TestSupport.Models.Character
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

    # Set sort_column_attnum for both tables
    characters_table = %{characters_table | sort_column_attnum: character_col_attnums["updated_at"]}

    characters_multi_pk_table = %{
      characters_multi_pk_table
      | sort_column_attnum: character_multi_pk_attnums["updated_at"]
    }

    characters_detailed_table = %{
      characters_detailed_table
      | sort_column_attnum: character_col_attnums["updated_at"]
    }

    ConnectionCache.cache_connection(db, Repo)

    character_sequence =
      DatabasesFactory.insert_sequence!(
        account_id: db.account_id,
        postgres_database_id: db.id,
        table_oid: characters_table.oid,
        sort_column_attnum: character_col_attnums["updated_at"]
      )

    character_multi_pk_sequence =
      DatabasesFactory.insert_sequence!(
        account_id: db.account_id,
        postgres_database_id: db.id,
        table_oid: characters_multi_pk_table.oid,
        sort_column_attnum: character_multi_pk_attnums["updated_at"]
      )

    character_sequence_filter =
      ConsumersFactory.sequence_filter(column_filters: [], group_column_attnums: Character.pk_attnums())

    character_multi_pk_sequence_filter =
      ConsumersFactory.sequence_filter(column_filters: [], group_column_attnums: CharacterMultiPK.pk_attnums())

    character_consumer =
      Repo.preload(
        ConsumersFactory.insert_sink_consumer!(
          sequence_id: character_sequence.id,
          sequence_filter: Map.from_struct(character_sequence_filter),
          account_id: db.account_id,
          postgres_database_id: db.id
        ),
        [:sequence, :postgres_database]
      )

    character_multi_pk_consumer =
      Repo.preload(
        ConsumersFactory.insert_sink_consumer!(
          sequence_id: character_multi_pk_sequence.id,
          sequence_filter: Map.from_struct(character_multi_pk_sequence_filter),
          account_id: db.account_id,
          postgres_database_id: db.id
        ),
        [:sequence, :postgres_database]
      )

    %{
      db: db,
      character_consumer: character_consumer,
      character_multi_pk_consumer: character_multi_pk_consumer,
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
      characters_table: table
    } do
      now = NaiveDateTime.utc_now()
      char1 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -3, :second))
      char2 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -2, :second))
      char3 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -1, :second))

      {:ok, _first_row, initial_cursor} = TableReader.fetch_first_row(db, table)

      {:ok, %{rows: records, next_cursor: next_cursor}} =
        TableReader.fetch_batch(db, table, initial_cursor, include_min: true)

      assert length(records) == 3
      assert_character_equal(Enum.at(records, 0), char1)
      assert_character_equal(Enum.at(records, 1), char2)
      assert_character_equal(Enum.at(records, 2), char3)

      # Verify next cursor matches last record
      assert next_cursor[table.sort_column_attnum] == char3.updated_at

      {:ok, %{rows: records, next_cursor: next_cursor}} =
        TableReader.fetch_batch(db, table, initial_cursor, include_min: false)

      assert length(records) == 2
      assert_character_equal(Enum.at(records, 0), char2)
      assert_character_equal(Enum.at(records, 1), char3)

      # Verify next cursor matches last record
      assert next_cursor[table.sort_column_attnum] == char3.updated_at
    end

    test "correctly handles nil and populated UUID, UUID[] and bytea fields", %{
      db: db,
      characters_detailed_table: table
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

      {:ok, %{rows: results, next_cursor: _next_cursor}} =
        TableReader.fetch_batch(
          db,
          table,
          initial_min_cursor,
          limit: 10,
          include_min: true
        )

      assert length(results) == 2

      # Find the results corresponding to our inserted characters
      result1 = Enum.find(results, &(&1["id"] == char1.id))
      result2 = Enum.find(results, &(&1["id"] == char2.id))

      # Verify enum handling
      assert result1["status"] == "active"
      assert result2["status"] == "retired"

      # Verify UUID handling
      refute result1["house_id"]
      assert {:ok, _} = UUID.info(result2["house_id"])
      assert result2["house_id"] == char2.house_id

      # Verify UUID array handling
      assert result1["related_houses"] == []
      assert length(result2["related_houses"]) == 2
      assert Enum.all?(result2["related_houses"], fn uuid -> match?({:ok, _}, UUID.info(uuid)) end)
      assert result2["related_houses"] == char2.related_houses

      # Verify bytea handling
      assert result1["binary_data"] == "\\x010203"
      assert result2["binary_data"] == "\\x040506"
    end

    test "respects the limit parameter", %{
      db: db,
      characters_table: table
    } do
      now = NaiveDateTime.utc_now()
      char1 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -2, :second))
      char2 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -1, :second))
      _char3 = CharacterFactory.insert_character!(updated_at: now)

      {:ok, _first_row, initial_cursor} = TableReader.fetch_first_row(db, table)

      {:ok, %{rows: records, next_cursor: next_cursor}} =
        TableReader.fetch_batch(db, table, initial_cursor, limit: 2, include_min: true)

      assert length(records) == 2
      assert_character_equal(Enum.at(records, 0), char1)
      assert_character_equal(Enum.at(records, 1), char2)

      # Verify next cursor matches last record in batch
      assert next_cursor[table.sort_column_attnum] == char2.updated_at
    end

    test "handles empty result set", %{
      db: db,
      characters_table: table
    } do
      cursor = KeysetCursor.min_cursor(table, NaiveDateTime.utc_now())

      {:ok, %{rows: records, next_cursor: next_cursor}} = TableReader.fetch_batch(db, table, cursor)

      assert records == []
      assert next_cursor == nil
    end
  end

  describe "fetch_batch primary_keys / by_primary_keys" do
    test "fetches primary keys and corresponding records for single PK table", %{
      db: db,
      character_consumer: consumer,
      characters_table: table
    } do
      now = NaiveDateTime.utc_now()
      char1 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -3, :second))
      char2 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -2, :second))
      char3 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -1, :second))

      {:ok, _first_row, initial_cursor} = TableReader.fetch_first_row(db, table)

      # Fetch primary keys
      {:ok, %{rows: primary_keys, next_cursor: next_cursor}} =
        TableReader.fetch_batch_primary_keys(db, table, initial_cursor, include_min: true)

      assert length(primary_keys) == 3
      assert primary_keys == [[to_string(char1.id)], [to_string(char2.id)], [to_string(char3.id)]]
      # Verify next cursor matches last record
      assert next_cursor[table.sort_column_attnum] == char3.updated_at

      # Fetch full records using primary keys
      {:ok, %{messages: messages}} = TableReader.fetch_batch_by_primary_keys(db, consumer, table, primary_keys)

      assert length(messages) == 3
      assert_character_equal(Enum.at(messages, 0).data.record, char1)
      assert_character_equal(Enum.at(messages, 1).data.record, char2)
      assert_character_equal(Enum.at(messages, 2).data.record, char3)
    end

    test "fetches primary keys and corresponding records for multi PK table", %{
      db: db,
      character_multi_pk_consumer: consumer,
      characters_multi_pk_table: table
    } do
      char1 = CharacterFactory.insert_character_multi_pk!()
      char2 = CharacterFactory.insert_character_multi_pk!()
      char3 = CharacterFactory.insert_character_multi_pk!()

      {:ok, _first_row, initial_cursor} = TableReader.fetch_first_row(db, table)

      # Fetch primary keys
      {:ok, %{rows: primary_keys, next_cursor: next_cursor}} =
        TableReader.fetch_batch_primary_keys(db, table, initial_cursor, include_min: true)

      assert length(primary_keys) == 3
      assert next_cursor != nil

      expected_pks = [
        CharacterMultiPK.record_pks(char1),
        CharacterMultiPK.record_pks(char2),
        CharacterMultiPK.record_pks(char3)
      ]

      assert primary_keys == Enum.map(expected_pks, fn pks -> Enum.map(pks, &to_string/1) end)

      # Fetch full records using primary keys
      {:ok, %{messages: messages}} = TableReader.fetch_batch_by_primary_keys(db, consumer, table, primary_keys)

      assert length(messages) == 3
      # Helper function to compare multi-PK records
      assert_multi_pk_character_equal(Enum.at(messages, 0).data.record, char1)
      assert_multi_pk_character_equal(Enum.at(messages, 1).data.record, char2)
      assert_multi_pk_character_equal(Enum.at(messages, 2).data.record, char3)
    end

    test "correctly paginates records with next_cursor for single PK table", %{
      db: db,
      character_consumer: consumer,
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
      {:ok, %{rows: primary_keys, next_cursor: first_page_cursor}} =
        TableReader.fetch_batch_primary_keys(db, table, initial_cursor,
          include_min: true,
          limit: 3
        )

      assert length(primary_keys) == 3
      assert Enum.all?(primary_keys, fn pks -> Enum.all?(pks, &is_binary/1) end)
      assert primary_keys == [[to_string(char1.id)], [to_string(char2.id)], [to_string(char3.id)]]
      assert first_page_cursor[table.sort_column_attnum] == char3.updated_at

      # Fetch first page of records and get next cursor
      {:ok, %{messages: messages, next_cursor: next_cursor}} =
        TableReader.fetch_batch_by_primary_keys(db, consumer, table, primary_keys)

      assert length(messages) == 3
      assert_character_equal(Enum.at(messages, 0).data.record, char1)
      assert_character_equal(Enum.at(messages, 1).data.record, char2)
      assert_character_equal(Enum.at(messages, 2).data.record, char3)
      assert next_cursor[table.sort_column_attnum] == char3.updated_at

      # Fetch second page using next_cursor
      {:ok, %{rows: primary_keys, next_cursor: second_page_cursor}} =
        TableReader.fetch_batch_primary_keys(db, table, next_cursor,
          include_min: false,
          limit: 3
        )

      assert length(primary_keys) == 2
      assert primary_keys == [[to_string(char4.id)], [to_string(char5.id)]]
      assert second_page_cursor[table.sort_column_attnum] == char5.updated_at

      # Fetch second page of records
      {:ok, %{messages: messages, next_cursor: final_cursor}} =
        TableReader.fetch_batch_by_primary_keys(db, consumer, table, primary_keys)

      assert length(messages) == 2
      assert_character_equal(Enum.at(messages, 0).data.record, char4)
      assert_character_equal(Enum.at(messages, 1).data.record, char5)
      assert final_cursor[table.sort_column_attnum] == char5.updated_at
    end
  end

  defp map_column_attnums(table) do
    Map.new(table.columns, fn column -> {column.name, column.attnum} end)
  end

  defp assert_character_equal(result, character) when is_struct(result) do
    assert_character_equal(Map.from_struct(result), character)
  end

  defp assert_character_equal(result, character) do
    assert_maps_equal(
      result,
      Map.from_struct(character),
      [
        "id",
        "name",
        "updated_at"
      ],
      indifferent_keys: true
    )
  end

  defp assert_multi_pk_character_equal(result, character) when is_struct(result) do
    assert_multi_pk_character_equal(Map.from_struct(result), character)
  end

  defp assert_multi_pk_character_equal(result, character) do
    assert_maps_equal(
      result,
      Map.from_struct(character),
      [
        "id_integer",
        "id_string",
        "id_uuid",
        "name",
        "house",
        "updated_at"
      ],
      indifferent_keys: true
    )
  end
end
