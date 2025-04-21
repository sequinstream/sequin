defmodule Sequin.Postgres.ArrayLexerTest do
  use ExUnit.Case, async: true

  alias Sequin.Postgres.ArrayLexer

  doctest ArrayLexer

  # ---------------------------------------------------------------------------
  describe "lex/1 – basic arrays" do
    test "parses empty array" do
      assert {:ok, []} = ArrayLexer.lex("{}")
    end

    test "parses array of integers" do
      assert {:ok, ["1", "2", "3"]} = ArrayLexer.lex("{1,2,3}")
    end

    test "parses array of strings" do
      assert {:ok, ["a", "b", "c"]} = ArrayLexer.lex(~s({"a","b","c"}))
    end

    test "parses array with NULL values" do
      assert {:ok, [nil, "hello", "42"]} = ArrayLexer.lex("{NULL,\"hello\",42}")
    end

    test "parses array with empty strings" do
      assert {:ok, ["", "x", ""]} = ArrayLexer.lex(~s({"","x",""}))
    end

    test "parses array with escaped quotes" do
      assert {:ok, ["a\"b", "c\\d"]} =
               ArrayLexer.lex(~s({"a\\"b","c\\\\d"}))
    end

    test "parses array with commas in strings" do
      assert {:ok, ["a,b", "c,d"]} = ArrayLexer.lex(~s({"a,b","c,d"}))
    end

    test "parses nested arrays" do
      assert {:ok, [["1", "2"], ["3", "4"]]} = ArrayLexer.lex("{{1,2},{3,4}}")
    end

    test "parses complex nested arrays" do
      assert {:ok, [["a", "b"], [nil, "c,d"]]} =
               ArrayLexer.lex(~s({{"a","b"},{NULL,"c,d"}}))
    end

    test "parses array with spaces" do
      assert {:ok, ["a", "b", "c"]} = ArrayLexer.lex(~s({ "a", "b" , "c" }))
    end
  end

  # ---------------------------------------------------------------------------
  describe "lex/1 – complex scenarios" do
    test "keeps jsonb elements as strings (no casting)" do
      jsonb_array =
        ~s({"[1,2,3]","{\\"a\\":\\"b\\",\\"c\\":\\"d\\"}","[{\\"x\\":1}]"})

      assert {:ok, ["[1,2,3]", ~s({"a":"b","c":"d"}), "[{\"x\":1}]"]} =
               ArrayLexer.lex(jsonb_array)
    end

    test "handles json values with nested escaped strings" do
      json_array = ~s({"{\\"name\\":\\"John\\\\\\\"Doe\\\\\\\"\\",\\"city\\":\\"New York\\"}"})

      assert {:ok, [~s({"name":"John\\"Doe\\"","city":"New York"})]} =
               ArrayLexer.lex(json_array)
    end

    test "handles deeply nested arrays" do
      nested_array = "{{{1,2},{3,4}},{{5,6},{7,8}}}"

      assert {:ok, [[["1", "2"], ["3", "4"]], [["5", "6"], ["7", "8"]]]} =
               ArrayLexer.lex(nested_array)
    end

    test "handles arrays with quoted strings containing braces" do
      array = ~s({"{nested}","not{nested"})
      assert {:ok, ["{nested}", "not{nested"]} = ArrayLexer.lex(array)
    end

    test "handles arrays with complex escaped sequences" do
      array_with_escaped_quotes = ~s({"a\\"b","c\\"d"})
      assert {:ok, ["a\"b", "c\"d"]} = ArrayLexer.lex(array_with_escaped_quotes)

      array_with_backslashes = ~s({"a\\\\b","c\\\\d"})
      assert {:ok, ["a\\b", "c\\d"]} = ArrayLexer.lex(array_with_backslashes)
    end

    test "handles arrays with consecutive empty strings" do
      array = ~s({"","",""})
      assert {:ok, ["", "", ""]} = ArrayLexer.lex(array)
    end

    test "handles array with only empty strings" do
      array = ~s({"",""})
      assert {:ok, ["", ""]} = ArrayLexer.lex(array)
    end

    test "handles array with multiple nesting levels" do
      array = "{{{{1,2}}}}"
      assert {:ok, [[[["1", "2"]]]]} = ArrayLexer.lex(array)
    end
  end

  # ---------------------------------------------------------------------------
  describe "lex/1 – error handling" do
    test "returns error tuple on malformed input" do
      # missing closing brace
      assert {:error, _reason} = ArrayLexer.lex("{1,2,3")
    end
  end
end
