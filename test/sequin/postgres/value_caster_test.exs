defmodule Sequin.Postgres.ValueCasterTest do
  use ExUnit.Case, async: true

  alias Sequin.Postgres.ValueCaster

  # ---------------------------------------------------------------------------
  describe "primitive scalars" do
    test "bool → boolean" do
      assert {:ok, true} = ValueCaster.cast("bool", "t")
      assert {:ok, false} = ValueCaster.cast("bool", "f")
    end

    test "numeric → integer / float / decimal" do
      assert {:ok, 42} = ValueCaster.cast("int4", "42")
      assert {:ok, 1.25} = ValueCaster.cast("float4", "1.25")
      assert {:ok, dec} = ValueCaster.cast("numeric", "9.99")
      assert Decimal.equal?(dec, Decimal.new("9.99"))
    end

    test "uuid → Ecto.UUID" do
      uuid = Ecto.UUID.generate()
      assert {:ok, ^uuid} = ValueCaster.cast("uuid", uuid)
    end
  end

  # ---------------------------------------------------------------------------
  describe "json & jsonb" do
    test "decodes valid json" do
      json = ~s({"a":1,"b":"two"})
      assert {:ok, %{"a" => 1, "b" => "two"}} = ValueCaster.cast("jsonb", json)
    end

    @tag capture_log: true
    test "returns error tuple on invalid json" do
      bad_json = ~s({"oops":})
      assert {:error, %{code: :invalid_json}} = ValueCaster.cast("json", bad_json)
    end
  end

  # ---------------------------------------------------------------------------
  describe "vectors" do
    test "casts pgvector literal to list of floats" do
      lit = "[0.1,0.2,0.3]"
      assert {:ok, [0.1, 0.2, 0.3]} = ValueCaster.cast("vector", lit)
      assert {:ok, nil} = ValueCaster.cast("vector", nil)
    end
  end

  # ---------------------------------------------------------------------------
  describe "arrays" do
    test "simple text array" do
      assert {:ok, ["a", "b", "c"]} =
               ValueCaster.cast("_text", ~s({"a","b","c"}))
    end

    test "integer array with NULLs" do
      assert {:ok, [1, nil, 3]} =
               ValueCaster.cast("_int4", "{1,NULL,3}")
    end

    test "nested 2‑D int array" do
      assert {:ok, [[1, 2], [3, 4]]} =
               ValueCaster.cast("_int4", "{{1,2},{3,4}}")
    end

    test "array of jsonb values" do
      jsonb_array =
        ~s({"{\\"x\\":1}","[2,3]","null"})

      assert {:ok, [%{"x" => 1}, [2, 3], nil]} =
               ValueCaster.cast("_jsonb", jsonb_array)
    end

    test "propagates lexer errors" do
      # missing closing brace
      malformed = "{1,2,3"

      assert {:error, %{message: "Invalid Postgres array" <> _err}} =
               ValueCaster.cast("_int4", malformed)
    end

    @tag capture_log: true
    test "propagates json errors from inside array" do
      array = ~s({"{\\"bad\\":}","1"})

      assert {:error, %{code: :invalid_json}} =
               ValueCaster.cast("_jsonb", array)
    end
  end

  # ---------------------------------------------------------------------------
  describe "fallback & unknown types" do
    test "unknown pg type ⇒ string passthrough" do
      assert {:ok, "whatever"} = ValueCaster.cast("mystery", "whatever")
    end
  end
end
