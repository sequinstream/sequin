defmodule Sequin.LoadGenerator do
  @moduledoc false
  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Postgres
  alias Sequin.Repo

  require Logger

  @doc """
  Sets up the load generator configuration, returning prepared workload data
  and example queries that will be executed.
  """
  def setup(sink_consumer_id, opts \\ []) do
    {sink_consumer, table, workload_data} = prepare_generation(sink_consumer_id, opts)

    # Generate example insert query
    if :inserts in Keyword.get(opts, :workload, [:inserts, :updates, :deletes]) do
      columns = Enum.reject(table.columns, & &1.is_pk?)
      column_names = Enum.map_join(columns, ", ", &Postgres.quote_name(&1.name))

      # Create placeholders for a single row
      values_placeholders = "(#{Enum.map_join(1..length(columns), ", ", &"$#{&1}")})"

      example_query = """
      INSERT INTO #{get_table_identifier(sink_consumer, table)}
      (#{column_names})
      VALUES #{values_placeholders}
      """

      # Get example values from first template
      example_values =
        case workload_data.inserts do
          %{templates: [first_template | _]} ->
            Enum.map(columns, &Map.get(first_template, &1.name))

          _ ->
            []
        end

      Logger.info("""
      Prepared load generator setup:
      Table: #{table.schema}.#{table.name}
      Example insert query:
      #{example_query}
      Example values: #{inspect(example_values)}
      """)
    end

    {:ok,
     %{
       sink_consumer: sink_consumer,
       table: table,
       workload_data: workload_data
     }}
  end

  def generate(sink_consumer_id, throughput_per_second, duration_ms, opts \\ []) do
    {sink_consumer, table, workload_data} = prepare_generation(sink_consumer_id, opts)
    batch_size = Keyword.get(opts, :batch_size, 100)
    workload = Keyword.get(opts, :workload, [:inserts, :updates, :deletes])

    # Calculate timing
    batches_per_second = throughput_per_second / batch_size
    total_batches = max(1, round(batches_per_second * (duration_ms / 1000)))

    now = System.system_time(:millisecond)
    interval = 1000 / batches_per_second

    # Create stream of work items
    work_items =
      Enum.map(1..total_batches, fn batch_num ->
        batch_workload = Enum.at(workload, rem(batch_num - 1, length(workload)))
        start_time = now + batch_num * interval

        {start_time, batch_workload, batch_size}
      end)

    # Start async workers
    work_items
    |> Task.async_stream(
      __MODULE__,
      :execute_workload,
      [sink_consumer, table, workload_data],
      max_concurrency: System.schedulers_online(),
      ordered: false
    )
    |> Stream.run()

    :ok
  end

  defp prepare_generation(sink_consumer_id, opts) do
    workload = Keyword.get(opts, :workload, [:inserts, :updates, :deletes])

    # Fetch sink consumer and preload necessary associations
    sink_consumer =
      sink_consumer_id
      |> Consumers.get_sink_consumer!()
      |> Repo.preload([:postgres_database, :sequence])

    # Get the target table from the database tables
    table =
      Enum.find(sink_consumer.postgres_database.tables, &(&1.oid == sink_consumer.sequence.table_oid))

    # Prepare workload data for each type
    workload_data = prepare_workloads(sink_consumer, table, workload)

    {sink_consumer, table, workload_data}
  end

  defp prepare_workloads(sink_consumer, table, workload_types) do
    Map.new(workload_types, fn type -> {type, prepare_workload(type, sink_consumer, table)} end)
  end

  defp prepare_workload(:inserts, sink_consumer, table) do
    case prepare_insert_workload(sink_consumer, table) do
      {:ok, data} ->
        data

      {:error, error} ->
        Logger.error("Failed to prepare insert workload: #{inspect(error)}")
        raise "Failed to prepare workload"
    end
  end

  defp prepare_workload(_type, _sink_consumer, _table) do
    # TODO: Implement prepare_update_workload and prepare_delete_workload
    %{}
  end

  @doc """
  Prepares workload data for insert operations.
  Returns a map of column values that can be used as templates for new inserts.
  """
  def prepare_insert_workload(sink_consumer, table, sample_size \\ 10_000) do
    Logger.info("Preparing insert workload for table #{table.name}")
    # Only exclude the auto-incrementing id column, keep other PKs
    columns = Enum.reject(table.columns, &(&1.is_pk? and &1.name == "id"))
    column_names = Enum.map_join(columns, ", ", &Postgres.quote_name(&1.name))

    # Start with a conservative 1% sample
    result = try_sample(sink_consumer, table, column_names, sample_size, 1)

    # If we didn't get enough rows, progressively increase sampling
    result =
      case result do
        {:ok, %{templates: templates}} when length(templates) < sample_size ->
          # Try 10%
          try_sample(sink_consumer, table, column_names, sample_size, 10)

        other ->
          other
      end

    # Final fallback to 50% if needed
    case result do
      {:ok, %{templates: templates}} when length(templates) < sample_size ->
        try_sample(sink_consumer, table, column_names, sample_size, 50)

      other ->
        other
    end
  end

  defp try_sample(sink_consumer, table, column_names, sample_size, percentage) do
    Logger.info("Sampling table #{table.name} at #{percentage}%")

    # Split the column_names string into a list and strip quotes
    column_list =
      column_names
      |> String.split(", ")
      |> Enum.map(&String.trim(&1, "\""))

    query = """
    SELECT #{column_names}
    FROM #{get_table_identifier(sink_consumer, table)}
    TABLESAMPLE SYSTEM(#{percentage})
    LIMIT #{sample_size}
    """

    case Postgres.query(sink_consumer.postgres_database, query, [], timeout: :infinity) do
      {:ok, %{rows: rows}} ->
        # Create template records by zipping column names with row values
        templates =
          Enum.map(rows, fn row ->
            column_list
            |> Enum.zip(row)
            |> Map.new(fn {col, val} -> {col, val} end)
          end)

        Logger.info("Prepared #{length(templates)} insert templates with #{percentage}% sampling")
        {:ok, %{templates: templates}}

      {:error, error} ->
        Logger.error("Failed to prepare insert workload at #{percentage}%: #{inspect(error)}")
        {:error, error}
    end
  end

  @doc """
  Executes a batch of insert operations.
  """
  def execute_insert_batch(sink_consumer, table, batch_size, workload_data) do
    # Only exclude the auto-incrementing id column, keep other PKs
    columns = Enum.reject(table.columns, &(&1.is_pk? and &1.name == "id"))
    column_names = Enum.map_join(columns, ", ", &Postgres.quote_name(&1.name))

    # Create placeholders for the VALUES clause
    # Each row will look like ($1, $2, $3), ($4, $5, $6), etc.
    values_placeholders =
      Enum.map_join(1..batch_size, ", ", fn batch_index ->
        start_param_index = (batch_index - 1) * length(columns) + 1
        params = start_param_index..(start_param_index + length(columns) - 1)
        "(#{Enum.map_join(params, ", ", &"$#{&1}")})"
      end)

    # Build the full INSERT query
    query = """
    INSERT INTO #{Postgres.quote_name(table.schema, table.name)}
    (#{column_names})
    VALUES #{values_placeholders}
    """

    # Generate parameter values by randomly selecting from templates
    params =
      Enum.flat_map(1..batch_size, fn _batch_index ->
        template = Enum.random(workload_data.templates)
        Enum.map(columns, &Map.get(template, &1.name))
      end)

    case Postgres.query(sink_consumer.postgres_database, query, params) do
      {:ok, result} ->
        {:ok, result}

      {:error, error} ->
        Logger.error("Failed to execute insert batch: #{inspect(error)}")
        {:error, error}
    end
  end

  def execute_workload({start_time, :inserts, batch_size}, sink_consumer, table, workload_data) do
    now = System.system_time(:millisecond)
    wait_time = start_time - now

    if wait_time < 0 do
      Logger.warning("Batch falling behind schedule by #{abs(wait_time)}ms")
    else
      Process.sleep(trunc(wait_time))
    end

    Logger.info("Executing batch of #{batch_size} insert operations")
    execute_insert_batch(sink_consumer, table, batch_size, workload_data.inserts)
  end

  def execute_workload({start_time, workload_type, batch_size}, _sink_consumer, _table, _workload_data) do
    now = System.system_time(:millisecond)
    wait_time = start_time - now

    if wait_time < 0 do
      Logger.warning("Batch falling behind schedule by #{abs(wait_time)}ms")
    else
      Process.sleep(trunc(wait_time))
    end

    Logger.info("Executing batch of #{batch_size} #{workload_type} operations")
    # TODO: Implement other workload types
    :ok
  end

  defp get_table_identifier(%SinkConsumer{id: "11fe121c-9462-43bd-8327-eb8d48393567"}, _table) do
    "public.holdings_acct_uuid_range16_008"
  end

  defp get_table_identifier(%SinkConsumer{}, table) do
    Postgres.quote_name(table.schema, table.name)
  end
end
