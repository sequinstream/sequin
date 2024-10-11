defmodule Sequin.TableProducerTest do
  use Sequin.DataCase, async: true

  alias Sequin.Databases
  alias Sequin.Databases.ConnectionCache
  alias Sequin.DatabasesRuntime.TableProducer
  alias Sequin.Factory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.DatabasesFactory

  setup do
    db = DatabasesFactory.insert_configured_postgres_database!(tables: [])
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

    consumer_id = "test_consumer_#{Factory.unique_integer()}"

    %{
      db: db,
      consumer_id: consumer_id,
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
      assert :error == TableProducer.fetch_cursors(consumer_id)
    end

    test "update_cursor and fetch_cursors work correctly", %{consumer_id: consumer_id} do
      min_cursor = %{1 => "2023-01-01", 2 => 123}
      max_cursor = %{1 => "2023-01-31", 2 => 456}

      assert :ok == TableProducer.update_cursor(consumer_id, :min, min_cursor)
      assert :ok == TableProducer.update_cursor(consumer_id, :max, max_cursor)

      assert {:ok, %{"min" => ^min_cursor, "max" => ^max_cursor}} = TableProducer.fetch_cursors(consumer_id)
    end

    test "cursor retrieves individual cursors", %{consumer_id: consumer_id} do
      min_cursor = %{1 => "2023-02-01", 2 => 789}
      max_cursor = %{1 => "2023-02-28", 2 => 1011}

      assert :ok == TableProducer.update_cursor(consumer_id, :min, min_cursor)
      assert :ok == TableProducer.update_cursor(consumer_id, :max, max_cursor)

      assert ^min_cursor = TableProducer.cursor(consumer_id, :min)
      assert ^max_cursor = TableProducer.cursor(consumer_id, :max)
    end

    test "cursor returns nil for non-existent cursor", %{consumer_id: consumer_id} do
      assert nil == TableProducer.cursor(consumer_id, :min)
      assert nil == TableProducer.cursor(consumer_id, :max)
    end

    test "delete_cursor removes all cursors", %{consumer_id: consumer_id} do
      min_cursor = %{1 => "2023-03-01", 2 => 1213}
      max_cursor = %{1 => "2023-03-31", 2 => 1415}

      assert :ok == TableProducer.update_cursor(consumer_id, :min, min_cursor)
      assert :ok == TableProducer.update_cursor(consumer_id, :max, max_cursor)

      assert {:ok, _} = TableProducer.fetch_cursors(consumer_id)

      assert :ok == TableProducer.delete_cursor(consumer_id)
      assert :error == TableProducer.fetch_cursors(consumer_id)
    end
  end

  describe "fetch_max_cursor/4" do
    test "fetches max cursor with timestamp sort_column", %{
      db: db,
      characters_table: table,
      character_col_attnums: attnums
    } do
      now = NaiveDateTime.utc_now()
      _char1 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -3, :second))
      _char2 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -2, :second))
      char3 = CharacterFactory.insert_character!(updated_at: NaiveDateTime.add(now, -1, :second))
      char4 = CharacterFactory.insert_character!(updated_at: now)

      {:ok, _first_row, initial_min_cursor} = TableProducer.fetch_first_row(db, table)
      limit = 3

      {:ok, cursor} = TableProducer.fetch_max_cursor(db, table, initial_min_cursor, limit: limit, include_min: true)

      assert char3.updated_at == NaiveDateTime.truncate(cursor[table.sort_column_attnum], :second)
      assert char3.id == cursor[attnums["id"]]

      {:ok, cursor} = TableProducer.fetch_max_cursor(db, table, initial_min_cursor, limit: limit, include_min: false)

      assert char4.updated_at == NaiveDateTime.truncate(cursor[table.sort_column_attnum], :second)
      assert char4.id == cursor[attnums["id"]]
    end

    test "fetches max cursor with compound primary key", %{
      db: db,
      characters_multi_pk_table: table,
      character_multi_pk_attnums: attnums
    } do
      now = NaiveDateTime.utc_now()
      _char1 = CharacterFactory.insert_character_multi_pk!(updated_at: NaiveDateTime.add(now, -2, :second))
      char2 = CharacterFactory.insert_character_multi_pk!(updated_at: now)
      char3 = CharacterFactory.insert_character_multi_pk!(updated_at: now)
      char4 = CharacterFactory.insert_character_multi_pk!(updated_at: now)

      {:ok, _first_row, initial_min_cursor} = TableProducer.fetch_first_row(db, table)
      limit = 3

      {:ok, cursor} = TableProducer.fetch_max_cursor(db, table, initial_min_cursor, limit: limit, include_min: true)

      assert_maps_equal(
        cursor,
        %{
          attnums["id_integer"] => char3.id_integer,
          attnums["id_string"] => char3.id_string,
          attnums["id_uuid"] => char3.id_uuid
        },
        ["id_integer", "id_string", "id_uuid"]
      )

      assert char3.updated_at == NaiveDateTime.truncate(cursor[table.sort_column_attnum], :second)

      # Test with a cursor to ensure we can move past records with the same updated_at
      cursor = create_cursor(char2, attnums)

      {:ok, cursor} = TableProducer.fetch_max_cursor(db, table, cursor, limit: limit, include_min: true)

      assert_maps_equal(
        cursor,
        %{
          attnums["id_integer"] => char4.id_integer,
          attnums["id_string"] => char4.id_string,
          attnums["id_uuid"] => char4.id_uuid
        },
        ["id_integer", "id_string", "id_uuid"]
      )

      assert char4.updated_at == NaiveDateTime.truncate(cursor[table.sort_column_attnum], :second)
    end
  end

  describe "fetch_records_in_range/5" do
    test "fetches records in range with compound primary key", %{
      db: db,
      characters_multi_pk_table: table,
      character_multi_pk_attnums: attnums
    } do
      now = NaiveDateTime.utc_now()
      char1 = CharacterFactory.insert_character_multi_pk!(updated_at: NaiveDateTime.add(now, -3, :second))
      char2 = CharacterFactory.insert_character_multi_pk!(updated_at: NaiveDateTime.add(now, -2, :second))
      char3 = CharacterFactory.insert_character_multi_pk!(updated_at: NaiveDateTime.add(now, -1, :second))
      char4 = CharacterFactory.insert_character_multi_pk!(updated_at: now)
      _char5 = CharacterFactory.insert_character_multi_pk!(updated_at: now)

      {:ok, _first_row, initial_min_cursor} = TableProducer.fetch_first_row(db, table)
      max_cursor = create_cursor(char4, attnums)

      limit = 10

      {:ok, results} =
        TableProducer.fetch_records_in_range(db, table, initial_min_cursor, max_cursor, limit: limit, include_min: true)

      assert length(results) == 4

      assert_character_equal(Enum.at(results, 0), char1)
      assert_character_equal(Enum.at(results, 1), char2)
      assert_character_equal(Enum.at(results, 2), char3)
      assert_character_equal(Enum.at(results, 3), char4)
    end

    test "fetches records in range with compound primary key and same timestamp", %{
      db: db,
      characters_multi_pk_table: table,
      character_multi_pk_attnums: attnums
    } do
      now = NaiveDateTime.utc_now()
      _char1 = CharacterFactory.insert_character_multi_pk!(updated_at: now)
      char2 = CharacterFactory.insert_character_multi_pk!(updated_at: now)
      char3 = CharacterFactory.insert_character_multi_pk!(updated_at: now)
      char4 = CharacterFactory.insert_character_multi_pk!(updated_at: now)
      _char5 = CharacterFactory.insert_character_multi_pk!(updated_at: now)

      min_cursor = create_cursor(char2, attnums)
      max_cursor = create_cursor(char4, attnums)

      limit = 10

      {:ok, results} =
        TableProducer.fetch_records_in_range(db, table, min_cursor, max_cursor, limit: limit)

      assert length(results) == 2

      assert_character_equal(Enum.at(results, 0), char3)
      assert_character_equal(Enum.at(results, 1), char4)
    end
  end

  describe "fetch_max_cursor and fetch_records_in_range combined" do
    test "processes all characters with same updated_at using small page size", %{
      db: db,
      characters_multi_pk_table: table
    } do
      # Insert 6 characters with the same updated_at
      now = NaiveDateTime.utc_now()

      characters =
        Enum.map(1..6, fn _ ->
          CharacterFactory.insert_character_multi_pk!(updated_at: now)
        end)

      page_size = 2
      {:ok, _first_row, initial_min_cursor} = TableProducer.fetch_first_row(db, table)
      processed_characters = []

      # Simulate processing all characters
      {processed_characters, _} =
        Enum.reduce_while(1..10, {processed_characters, initial_min_cursor}, fn _, {acc, current_cursor} ->
          include_min = current_cursor == initial_min_cursor

          case TableProducer.fetch_max_cursor(db, table, current_cursor,
                 limit: page_size,
                 include_min: include_min
               ) do
            {:ok, nil} ->
              {:halt, {acc, current_cursor}}

            {:ok, max_cursor} ->
              {:ok, records} =
                TableProducer.fetch_records_in_range(
                  db,
                  table,
                  current_cursor,
                  max_cursor,
                  limit: page_size,
                  include_min: include_min
                )

              new_acc = acc ++ records
              {:cont, {new_acc, max_cursor}}
          end
        end)

      # Verify that all characters were processed
      assert length(processed_characters) == 6

      # Verify that all original characters are in the processed list
      assert_lists_equal(characters, processed_characters, fn char, processed ->
        char.id_integer == processed["id_integer"] &&
          char.id_string == processed["id_string"] &&
          char.id_uuid == processed["id_uuid"]
      end)
    end
  end

  describe "fetch_first_row/2" do
    test "fetches the first row and initial min cursor for characters_multi_pk table", %{
      db: db,
      characters_multi_pk_table: table,
      character_multi_pk_attnums: attnums
    } do
      # Insert a character with multiple primary keys
      char = CharacterFactory.insert_character_multi_pk!()

      # Fetch the first row
      {:ok, first_row, initial_min_cursor} = TableProducer.fetch_first_row(db, table)

      # Assert that the first_row matches the inserted character
      assert first_row["id_integer"] == char.id_integer
      assert first_row["id_string"] == char.id_string
      assert first_row["id_uuid"] == char.id_uuid
      assert first_row["name"] == char.name
      assert NaiveDateTime.compare(first_row["updated_at"], char.updated_at) == :eq

      # Assert that the initial_min_cursor is correct
      assert NaiveDateTime.truncate(initial_min_cursor[table.sort_column_attnum], :second) == char.updated_at
      assert initial_min_cursor[attnums["id_integer"]] == char.id_integer
      assert initial_min_cursor[attnums["id_string"]] == char.id_string
      assert initial_min_cursor[attnums["id_uuid"]] == char.id_uuid
    end

    test "returns nil when the table is empty", %{
      db: db,
      characters_table: table
    } do
      # Ensure the table is empty
      Repo.delete_all(Sequin.Test.Support.Models.Character)

      # Fetch the first row
      assert {:ok, nil, nil} = TableProducer.fetch_first_row(db, table)
    end
  end

  describe "fetch_records_in_range/5 with UUID columns" do
    test "correctly handles nil and populated UUID fields", %{
      db: db,
      characters_detailed_table: table
    } do
      # Insert two characters, one with nil house_id and one with populated house_id
      char1 = CharacterFactory.insert_character_detailed!(house_id: nil)
      char2 = CharacterFactory.insert_character_detailed!(house_id: UUID.uuid4())

      {:ok, _first_row, initial_min_cursor} = TableProducer.fetch_first_row(db, table)
      {:ok, max_cursor} = TableProducer.fetch_max_cursor(db, table, initial_min_cursor, limit: 10)

      {:ok, results} =
        TableProducer.fetch_records_in_range(
          db,
          table,
          initial_min_cursor,
          max_cursor,
          limit: 10,
          include_min: true
        )

      assert length(results) == 2

      # Find the results corresponding to our inserted characters
      result1 = Enum.find(results, &(&1["id"] == char1.id))
      result2 = Enum.find(results, &(&1["id"] == char2.id))

      # Verify that the house_id is correctly nil for the first character
      refute result1["house_id"]

      # Verify that the house_id is a valid UUID for the second character
      assert {:ok, _} = UUID.info(result2["house_id"])
      assert result2["house_id"] == char2.house_id
    end
  end

  defp map_column_attnums(table) do
    Map.new(table.columns, fn column -> {column.name, column.attnum} end)
  end

  defp create_cursor(character, attnums) do
    %{
      attnums["updated_at"] => character.updated_at,
      attnums["id_integer"] => character.id_integer,
      attnums["id_string"] => character.id_string,
      attnums["id_uuid"] => character.id_uuid
    }
  end

  defp assert_character_equal(result, character) do
    assert_maps_equal(
      result,
      Map.from_struct(character),
      [
        "id_integer",
        "id_string",
        "id_uuid",
        "name"
      ],
      indifferent_keys: true
    )
  end
end
