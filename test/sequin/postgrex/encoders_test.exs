defmodule Sequin.Postgrex.EncodersTest do
  use ExUnit.Case, async: true

  # Ensure the encoders are loaded
  require Sequin.Postgrex.Encoders

  describe "Jason.Encoder for Postgrex.Interval" do
    test "encodes interval with all fields" do
      interval = %Postgrex.Interval{
        months: 1,
        days: 2,
        secs: 3600,
        microsecs: 500_000
      }

      decoded = Jason.decode!(Jason.encode!(interval))

      assert decoded == %{
               "months" => 1,
               "days" => 2,
               "secs" => 3600,
               "microsecs" => 500_000
             }
    end

    test "encodes interval with zero values" do
      interval = %Postgrex.Interval{
        months: 0,
        days: 0,
        secs: 0,
        microsecs: 0
      }

      decoded = Jason.decode!(Jason.encode!(interval))

      assert decoded == %{
               "months" => 0,
               "days" => 0,
               "secs" => 0,
               "microsecs" => 0
             }
    end

    test "encodes interval with only months" do
      interval = %Postgrex.Interval{
        months: 12,
        days: 0,
        secs: 0,
        microsecs: 0
      }

      decoded = Jason.decode!(Jason.encode!(interval))

      assert decoded == %{
               "months" => 12,
               "days" => 0,
               "secs" => 0,
               "microsecs" => 0
             }
    end

    test "encodes interval with negative values" do
      interval = %Postgrex.Interval{
        months: -1,
        days: -5,
        secs: -3600,
        microsecs: 0
      }

      decoded = Jason.decode!(Jason.encode!(interval))

      assert decoded == %{
               "months" => -1,
               "days" => -5,
               "secs" => -3600,
               "microsecs" => 0
             }
    end
  end

  describe "Jason.Encoder for Postgrex.Lexeme" do
    test "encodes lexeme with positions" do
      lexeme = %Postgrex.Lexeme{
        word: "hello",
        positions: [{1, :A}, {5, :B}]
      }

      decoded = Jason.decode!(Jason.encode!(lexeme))

      assert decoded == %{
               "type" => "lexeme",
               "word" => "hello",
               "positions" => [[1, "A"], [5, "B"]]
             }
    end
  end

  describe "String.Chars for Postgrex.Range" do
    test "converts daterange to string" do
      range = %Postgrex.Range{
        lower: ~D[2020-01-01],
        upper: ~D[2023-12-31],
        lower_inclusive: true,
        upper_inclusive: false
      }

      assert to_string(range) == "[2020-01-01,2023-12-31)"
    end

    test "converts range with nil bounds" do
      range = %Postgrex.Range{
        lower: nil,
        upper: ~D[2023-12-31],
        lower_inclusive: false,
        upper_inclusive: true
      }

      assert to_string(range) == "(,2023-12-31]"
    end
  end

  describe "Jason.Encoder for Postgrex.Range" do
    test "encodes range as string" do
      range = %Postgrex.Range{
        lower: ~D[2020-01-01],
        upper: ~D[2023-12-31],
        lower_inclusive: true,
        upper_inclusive: false
      }

      result = Jason.encode!(range)

      assert result == ~s|"[2020-01-01,2023-12-31)"|
    end
  end
end
