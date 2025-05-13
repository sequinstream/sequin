defmodule Sequin.ChangesetTest do
  use Sequin.Case, async: true

  import Ecto.Changeset

  alias Sequin.Changeset

  describe "put_deserializers/3 and deserialize/2 integration" do
    test "properly handles serialization and deserialization of complex types" do
      # Create a record with complex types
      now = DateTime.truncate(DateTime.utc_now(), :second)
      today = Date.utc_today()
      naive_now = NaiveDateTime.truncate(NaiveDateTime.utc_now(), :second)
      decimal_value = Decimal.new("123.45")

      # Build initial data for schemaless changeset
      data = %{
        record: %{
          "id" => 1,
          "name" => "Test Record",
          "amount" => decimal_value,
          "created_at" => now,
          "date" => today,
          "updated_at" => naive_now
        }
      }

      # Define types for schemaless changeset
      types = %{
        record: :map,
        record_deserializers: :map
      }

      # Create a schemaless changeset
      changeset =
        {data, types}
        |> cast(%{}, [:record])
        |> Changeset.put_deserializers(:record, :record_deserializers)

      # Verify that deserializers are correctly generated
      deserializers =
        changeset
        |> get_change(:record_deserializers)
        |> Map.new()

      assert Map.has_key?(deserializers, "amount")
      assert Map.has_key?(deserializers, "created_at")
      assert Map.has_key?(deserializers, "date")
      assert Map.has_key?(deserializers, "updated_at")

      assert deserializers["amount"] == "decimal"
      assert deserializers["created_at"] == "datetime"
      assert deserializers["date"] == "date"
      assert deserializers["updated_at"] == "naive_datetime"

      # Simulate the Repo insert and query
      json_record = JSON.decode!(JSON.encode!(data.record))

      # Now deserialize the JSON-like map back to complex types
      deserialized_record = Changeset.deserialize(json_record, deserializers)

      # Verify all complex types are properly converted back
      assert deserialized_record["id"] == 1
      assert deserialized_record["name"] == "Test Record"

      assert %Decimal{} = deserialized_record["amount"]
      assert Decimal.equal?(deserialized_record["amount"], decimal_value)

      assert %DateTime{} = deserialized_record["created_at"]
      assert DateTime.compare(deserialized_record["created_at"], now) == :eq

      assert %Date{} = deserialized_record["date"]
      assert Date.compare(deserialized_record["date"], today) == :eq

      assert %NaiveDateTime{} = deserialized_record["updated_at"]
      assert NaiveDateTime.compare(deserialized_record["updated_at"], naive_now) == :eq
    end
  end
end
