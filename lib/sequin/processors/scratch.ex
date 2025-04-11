defmodule Sequin.Processors.Scratch do
  @moduledoc false
  alias Sequin.Processors.Starlark

  def go do
    :ok = Starlark.load_code(File.read!("lib/sequin/processors/example.star"))

    # Sample data with some regular fields and some secret fields
    record = %{
      "id" => 123,
      "name" => "Test Record",
      "value" => 99.9,
      "secret_key" => "abc123",
      "secret_token" => "xyz789",
      "timestamp" => "2023-08-15T12:00:00Z"
    }

    changes = %{
      "value" => 100.0,
      "status" => "updated",
      "secret_timestamp" => "2023-08-15"
    }

    # Convert Elixir maps to JSON strings for Starlark
    record_json = Jason.encode!(record)
    changes_json = Jason.encode!(changes)

    dbg(record_json)
    dbg(changes_json)

    # Call the transform function with our sample data
    case Starlark.call_function("transform", [record_json, changes_json]) do
      {:ok, result} ->
        IO.puts("Transform result:")
        IO.inspect(result)

        # You can access the filtered data
        IO.puts("\nFiltered record:")
        IO.inspect(result["record"])

        IO.puts("\nFiltered changes:")
        IO.inspect(result["changes"])

      {:error, error} ->
        IO.puts("Error calling transform function: #{error}")
    end
  end
end
