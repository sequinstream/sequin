defmodule Sequin.Postgrex.EncodersTest do
  use ExUnit.Case, async: true

  # Ensure the encoders are loaded
  require Sequin.Postgrex.Encoders

  describe "Jason.Encoder for Postgrex.Interval " do
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

    test "encodes 1 day as 24:00:00" do
      interval = %Postgrex.Interval{
        months: 0,
        days: 1,
        secs: 0,
        microsecs: 0
      }

      # 1 day = 24 hours
      assert Jason.encode!(interval) == ~s|"24:00:00"|
    end

    test "encodes days and time combined" do
      interval = %Postgrex.Interval{
        months: 0,
        days: 1,
        secs: 3600,
        microsecs: 0
      }

      # 1 day + 1 hour = 25 hours
      assert Jason.encode!(interval) == ~s|"25:00:00"|
    end

    test "encodes 1 month as 720 hours (30 days)" do
      interval = %Postgrex.Interval{
        months: 1,
        days: 0,
        secs: 0,
        microsecs: 0
      }

      # 1 month = 30 days = 720 hours
      assert Jason.encode!(interval) == ~s|"720:00:00"|
    end

    test "encodes complex interval with months, days, and time" do
      interval = %Postgrex.Interval{
        months: 1,
        days: 2,
        secs: 3661,
        microsecs: 0
      }

      # 1 month (720h) + 2 days (48h) + 1h1m1s = 769:01:01
      assert Jason.encode!(interval) == ~s|"769:01:01"|
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

    test "encodes negative days as negative hours" do
      interval = %Postgrex.Interval{
        months: 0,
        days: -1,
        secs: 0,
        microsecs: 0
      }

      # -1 day = -24 hours
      assert Jason.encode!(interval) == ~s|"-24:00:00"|
    end

    test "encodes large intervals correctly" do
      interval = %Postgrex.Interval{
        months: 0,
        days: 0,
        secs: 100 * 3600,
        microsecs: 0
      }

      # 100 hours
      assert Jason.encode!(interval) == ~s|"100:00:00"|
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
