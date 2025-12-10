defmodule Sequin.Postgrex.EncodersTest do
  use ExUnit.Case, async: true

  # Ensure the encoders are loaded
  require Sequin.Postgrex.Encoders

  describe "Jason.Encoder for Postgrex.Interval" do
    test "encodes simple time interval as HH:MM:SS" do
      interval = %Postgrex.Interval{
        months: 0,
        days: 0,
        secs: 3661,
        microsecs: 0
      }

      assert Jason.encode!(interval) == ~s|"01:01:01"|
    end

    test "encodes zero interval as 00:00:00" do
      interval = %Postgrex.Interval{
        months: 0,
        days: 0,
        secs: 0,
        microsecs: 0
      }

      assert Jason.encode!(interval) == ~s|"00:00:00"|
    end

    test "encodes interval with months, days, and time" do
      interval = %Postgrex.Interval{
        months: 1,
        days: 2,
        secs: 3661,
        microsecs: 0
      }

      assert Jason.encode!(interval) == ~s|"1 mon 2 days 01:01:01"|
    end

    test "encodes interval with years" do
      interval = %Postgrex.Interval{
        months: 14,
        days: 0,
        secs: 0,
        microsecs: 0
      }

      # 14 months = 1 year 2 months (no time portion when zero)
      assert Jason.encode!(interval) == ~s|"1 year 2 mons"|
    end

    test "encodes interval with microseconds" do
      interval = %Postgrex.Interval{
        months: 0,
        days: 0,
        secs: 1,
        microsecs: 500_000
      }

      assert Jason.encode!(interval) == ~s|"00:00:01.5"|
    end

    test "encodes interval with microseconds preserving precision" do
      interval = %Postgrex.Interval{
        months: 0,
        days: 0,
        secs: 0,
        microsecs: 123_456
      }

      assert Jason.encode!(interval) == ~s|"00:00:00.123456"|
    end

    test "encodes negative time interval" do
      interval = %Postgrex.Interval{
        months: 0,
        days: 0,
        secs: -3661,
        microsecs: 0
      }

      assert Jason.encode!(interval) == ~s|"-01:01:01"|
    end

    test "encodes complex interval with negative values" do
      interval = %Postgrex.Interval{
        months: -1,
        days: -5,
        secs: 0,
        microsecs: 0
      }

      # No time portion when zero
      assert Jason.encode!(interval) == ~s|"-1 mon -5 days"|
    end

    test "encodes singular units correctly" do
      interval = %Postgrex.Interval{
        months: 1,
        days: 1,
        secs: 0,
        microsecs: 0
      }

      # No time portion when zero
      assert Jason.encode!(interval) == ~s|"1 mon 1 day"|
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
