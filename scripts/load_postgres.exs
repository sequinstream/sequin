Mix.install([
  # Database and Ecto
  {:ecto_sql, "~> 3.10"},
  {:postgrex, ">= 0.0.0"}
])

# Configuration constants
defmodule LoadPostgres do
  @moduledoc false
  @target_accumulated_messages 100_000
  @batch_size 100
  @total_throughput 10000
  @duration_seconds 300
  @concurrency 20

  # Generate random string for names
  defp random_string(length \\ 10) do
    length
    |> :crypto.strong_rand_bytes()
    |> Base.encode16()
    |> binary_part(0, length)
  end

  # Insert new rows with random names
  defp insert_batch(conn, batch_size) do
    names = for _ <- 1..batch_size, do: random_string()

    values = Enum.map_join(names, ", ", fn name -> "(DEFAULT, '#{name}', 0)" end)

    query = "INSERT INTO load_test_table_1 (id, name, idx) VALUES #{values}"
    Postgrex.query!(conn, query, [])

    batch_size
  end

  # Update existing rows by incrementing idx
  defp update_batch(conn, batch_size) do
    query = """
    UPDATE load_test_table_1
    SET idx = idx + 1
    WHERE id IN (
      SELECT id FROM load_test_table_1
      ORDER BY RANDOM()
      LIMIT #{batch_size}
    )
    """

    result = Postgrex.query!(conn, query, [])
    result.num_rows
  end

  # Delete rows to maintain target count
  defp delete_batch(conn, batch_size) do
    query = """
    DELETE FROM load_test_table_1
    WHERE id IN (
      SELECT id FROM load_test_table_1
      ORDER BY RANDOM()
      LIMIT #{batch_size}
    )
    """

    result = Postgrex.query!(conn, query, [])
    result.num_rows
  end

  # Get current row count
  defp get_row_count(conn) do
    result = Postgrex.query!(conn, "SELECT COUNT(*) FROM load_test_table_1", [])
    result.rows |> List.first() |> List.first()
  end

  # Main load testing function
  def run_load_test(conn) do
    IO.puts("Starting load test...")
    IO.puts("Target accumulated messages: #{@target_accumulated_messages}")
    IO.puts("Batch size: #{@batch_size}")
    IO.puts("Total throughput: #{@total_throughput}")
    IO.puts("Concurrency: #{@concurrency}")

    # Start by truncating the table
    IO.puts("\nTruncating table...")
    Postgrex.query!(conn, "TRUNCATE load_test_table_1 RESTART IDENTITY", [])

    # Phase 1: Insert until we reach target accumulated messages
    IO.puts("\nPhase 1: Inserting messages to reach target...")
    insert_until_target(conn)

    # Phase 2: Maintain target count while processing total throughput
    IO.puts("\nPhase 2: Processing total throughput while maintaining target count...")
    process_throughput(conn)

    IO.puts("\nLoad test completed!")
  end

  defp insert_until_target(conn) do
    current_count = get_row_count(conn)

    if current_count < @target_accumulated_messages do
      needed_messages = @target_accumulated_messages - current_count
      batches_needed = ceil(needed_messages / @batch_size)

      IO.puts("Need #{batches_needed} batches to reach target...")

      for batch_num <- 1..batches_needed do
        inserted = insert_batch(conn, @batch_size)
        current_count = get_row_count(conn)
        IO.puts("Batch #{batch_num}: Inserted #{inserted} rows. Total: #{current_count}")
      end
    end
  end

  defp process_throughput(conn) do
    operations_per_second = @total_throughput / @batch_size
    total_operations = round(@duration_seconds * operations_per_second)
    ms_per_operation = round(1000 / operations_per_second * @concurrency)

    # Process operations with concurrency
    1..total_operations
    |> Stream.map(fn _ ->
      case :rand.uniform() do
        x when x <= 0.6 -> :update
        x when x <= 0.9 -> :insert
        _ -> :delete
      end
    end)
    |> Task.async_stream(
      fn operation ->
        start_time = System.monotonic_time(:millisecond)
        case operation do
          :insert ->
            insert_batch(conn, @batch_size)

          :update ->
            update_batch(conn, @batch_size)

          :delete ->
            delete_batch(conn, @batch_size)
        end

        end_time = System.monotonic_time(:millisecond)
        duration = end_time - start_time
        to_sleep = max(0, ms_per_operation - duration)
        Process.sleep(to_sleep)
      end,
      max_concurrency: @concurrency,
      ordered: false,
      timeout: 30_000
    )
    |> Stream.map(fn {:ok, result} -> result end)
    |> Stream.chunk_every(ceil(operations_per_second * @concurrency))
    |> Enum.each(fn _chunk ->
      current_count = get_row_count(conn)
      IO.puts("Processed batch. Current row count: #{current_count}")
    end)

    # Keep connection alive for a moment to see final results
    Process.sleep(1000)
    final_count = get_row_count(conn)
    IO.puts("\nFinal row count: #{final_count}")
  end
end

{:ok, conn} =
  Postgrex.start_link(
    database: "dune",
    username: "postgres",
    password: "postgres",
    hostname: "localhost",
    port: 5432
  )

# Setup the load test table with proper schema
Postgrex.query!(
  conn,
  "CREATE TABLE IF NOT EXISTS load_test_table_1 (id SERIAL PRIMARY KEY, name VARCHAR(255), idx INTEGER DEFAULT 0)",
  []
)

# Run the load test
LoadPostgres.run_load_test(conn)

# Close connection
GenServer.stop(conn)
