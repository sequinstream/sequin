defmodule Mix.Tasks.Bench do
  @shortdoc "Run end-to-end pipeline benchmark"
  @moduledoc """
  Runs an end-to-end benchmark of the Sequin pipeline using BenchmarkSource
  and BenchmarkPipeline.

  ## Usage

      mix benchmark [options]

  ## Options

    * `--duration` - Duration to run benchmark in seconds (default: 60)
    * `--row-sizes` - Row size distribution as "fraction:bytes,..." (default: "1.0:200")
    * `--transaction-sizes` - Transaction size distribution as "fraction:count,..." (default: "1.0:10")
    * `--pk-collision-rate` - PK collision rate 0.0-1.0 (default: 0.005)
    * `--partition-count` - Number of partitions (default: schedulers_online)
    * `--max-messages` - Maximum messages to generate (default: unlimited)
    * `--through` - Pipeline stage to run through: "full" or "reorder_buffer" (default: "full")

  ## Examples

      # Run with defaults (60s duration)
      mix benchmark

      # Run for 30 seconds with larger rows
      mix benchmark --duration 30 --row-sizes "0.9:200,0.1:10000"

      # Run with more partitions
      mix benchmark --partition-count 16

      # Run with exactly 1000 messages (useful for debugging)
      mix benchmark --max-messages 1000

      # Run through reorder_buffer only (isolated pipeline test)
      mix benchmark --through reorder_buffer
  """
  use Mix.Task

  alias Sequin.Accounts
  alias Sequin.Benchmark.Stats
  alias Sequin.Consumers
  alias Sequin.Databases
  alias Sequin.Postgres.BenchmarkSource
  alias Sequin.Postgres.VirtualBackend
  alias Sequin.Replication
  alias Sequin.Repo
  alias Sequin.Runtime.SlotProducer.Batch
  alias Sequin.Runtime.SlotProducer.Supervisor, as: SlotProducerSupervisor
  alias Sequin.Runtime.SlotSupervisor

  require Logger

  @green "\e[32m"
  @yellow "\e[33m"
  @cyan "\e[36m"
  @reset "\e[0m"
  @bold "\e[1m"

  @default_duration 60
  @default_row_sizes [{1.0, 200}]
  @default_transaction_sizes [{1.0, 10}]
  @default_pk_collision_rate 0.005
  @default_partition_count System.schedulers_online()
  @slot_name "sequin_bench_slot"
  @pub_name "sequin_bench_pub"

  def run(args) do
    # Parse arguments
    {opts, _, _} =
      OptionParser.parse(args,
        strict: [
          duration: :integer,
          row_sizes: :string,
          transaction_sizes: :string,
          pk_collision_rate: :float,
          partition_count: :integer,
          max_messages: :integer,
          through: :string
        ]
      )

    duration = Keyword.get(opts, :duration, @default_duration)
    row_sizes = parse_distribution(Keyword.get(opts, :row_sizes), @default_row_sizes)
    transaction_sizes = parse_distribution(Keyword.get(opts, :transaction_sizes), @default_transaction_sizes)
    pk_collision_rate = Keyword.get(opts, :pk_collision_rate, @default_pk_collision_rate)
    partition_count = Keyword.get(opts, :partition_count, @default_partition_count)
    max_messages = Keyword.get(opts, :max_messages)
    through = opts |> Keyword.get(:through, "full") |> String.to_existing_atom()

    # Start the application
    Mix.Task.run("app.start")

    announce("#{@bold}=== Sequin Pipeline Benchmark ===#{@reset}", @cyan)
    IO.puts("")

    announce("Configuration:", @yellow)
    IO.puts("  Duration: #{duration}s")
    IO.puts("  Row sizes: #{inspect(row_sizes)}")
    IO.puts("  Transaction sizes: #{inspect(transaction_sizes)}")
    IO.puts("  PK collision rate: #{pk_collision_rate}")
    IO.puts("  Partition count: #{partition_count}")
    IO.puts("  Max messages: #{max_messages || "unlimited"}")
    IO.puts("  Through: #{through}")
    IO.puts("")

    # Setup replication slot
    Repo.query!("drop publication if exists #{@pub_name}")
    Repo.query!("create publication #{@pub_name} for all tables with (publish_via_partition_root = true)")

    case Repo.query("select pg_drop_replication_slot($1)", [@slot_name]) do
      {:ok, _} -> :ok
      {:error, %Postgrex.Error{postgres: %{code: :undefined_object}}} -> :ok
    end

    Repo.query!("select pg_create_logical_replication_slot($1, 'pgoutput')::text", [@slot_name])

    # Create test entities
    announce("Setting up test entities...", @yellow)
    {replication, source_id} = setup_base_entities()
    IO.puts("  Replication slot: #{replication.id}")

    # For :full mode, we need a consumer for Broadway
    # For :reorder_buffer mode, we use the replication.id as the checksum owner
    {consumer, checksum_owner_id} =
      case through do
        :full ->
          consumer = setup_consumer(replication, partition_count)
          IO.puts("  Consumer: #{consumer.id}")
          {consumer, consumer.id}

        :reorder_buffer ->
          # Initialize checksums with replication.id as owner
          Stats.init_for_owner(replication.id, partition_count)
          {nil, replication.id}
      end

    IO.puts("")

    # Start BenchmarkSource
    announce("Starting BenchmarkSource...", @yellow)

    source_opts =
      Sequin.Keyword.put_if_present(
        [
          id: source_id,
          row_sizes: row_sizes,
          transaction_sizes: transaction_sizes,
          pk_collision_rate: pk_collision_rate,
          partition_count: partition_count
        ],
        :max_messages,
        max_messages
      )

    {:ok, _source_pid} = BenchmarkSource.start_link(source_opts)

    # Start pipeline based on --through mode
    announce("Starting pipeline...", @yellow)

    case through do
      :full ->
        {:ok, _slot_sup} =
          SlotSupervisor.start_link(
            pg_replication_id: replication.id,
            slot_producer_opts: [
              backend_mod: VirtualBackend,
              connect_opts: [id: source_id, source_mod: BenchmarkSource]
            ]
          )

      :reorder_buffer ->
        replication = Repo.preload(replication, :postgres_database)

        # Initialize stats tracking for this owner
        Stats.init_for_owner(checksum_owner_id, partition_count)

        flush_batch_fn = mock_flush_batch_fn(partition_count, checksum_owner_id)

        {:ok, _slot_sup} =
          SlotProducerSupervisor.start_link(
            replication_slot: replication,
            slot_producer_opts: [
              backend_mod: VirtualBackend,
              connect_opts: [id: source_id, source_mod: BenchmarkSource]
            ],
            reorder_buffer_opts: [
              flush_batch_fn: flush_batch_fn
            ],
            slot_processor_opts: [
              skip_start?: true
            ]
          )
    end

    IO.puts("")

    # Record start metrics
    start_time = System.monotonic_time(:millisecond)

    start_metrics =
      if consumer do
        capture_metrics(consumer.id, consumer.name)
      else
        # For :reorder_buffer mode, capture from Metrics module
        capture_metrics_from_module(checksum_owner_id)
      end

    announce("Running benchmark for #{duration}s...", @green)
    Process.sleep(duration * 1000)

    # Pause the source and wait for pipeline to drain
    announce("Pausing source and waiting for pipeline to drain...", @yellow)
    BenchmarkSource.pause(source_id)

    # Get source count immediately after pause
    source_checksums = BenchmarkSource.checksums(source_id)
    source_count = source_checksums |> Map.values() |> Enum.map(&elem(&1, 1)) |> Enum.sum()
    IO.puts("  Source message count at pause: #{source_count}")

    wait_for_pipeline_drain(checksum_owner_id)

    # Record end metrics
    end_time = System.monotonic_time(:millisecond)

    end_metrics =
      if consumer do
        capture_metrics(consumer.id, consumer.name)
      else
        # For :reorder_buffer mode, capture from Metrics module
        capture_metrics_from_module(checksum_owner_id)
      end

    actual_duration_s = (end_time - start_time) / 1000

    IO.puts("")

    # Get final checksums - re-fetch in case source had buffered messages
    source_checksums = BenchmarkSource.checksums(source_id)
    pipeline_checksums = Stats.checksums(checksum_owner_id)

    # Get tracked messages for comparison
    source_tracked = BenchmarkSource.tracked_messages(source_id)
    pipeline_tracked = Stats.tracked_messages(checksum_owner_id)

    # Print results
    print_results(
      start_metrics,
      end_metrics,
      actual_duration_s,
      source_checksums,
      pipeline_checksums,
      source_tracked,
      pipeline_tracked
    )

    # Cleanup
    cleanup_entities(consumer, replication)
  end

  defp cleanup_entities(_consumer, replication) do
    {:ok, database} = Databases.get_db(replication.postgres_database_id)
    {:ok, account} = Accounts.get_account(database.account_id)

    # This stops all processes and deletes all entities
    Accounts.delete_account_and_account_resources(account)
  end

  # Wait until message count stabilizes
  defp wait_for_pipeline_drain(consumer_id, stable_for \\ 2000) do
    before = pipeline_total_count(consumer_id)
    Process.sleep(stable_for)
    after_count = pipeline_total_count(consumer_id)

    if after_count == before do
      IO.puts("  Pipeline drained (#{after_count} messages)")
    else
      IO.puts("  Still draining: #{before} -> #{after_count}")
      wait_for_pipeline_drain(consumer_id, stable_for)
    end
  end

  defp pipeline_total_count(consumer_id) do
    consumer_id
    |> Stats.checksums()
    |> Map.values()
    |> Enum.map(&elem(&1, 1))
    |> Enum.sum()
  end

  defp setup_base_entities do
    # Create account using domain function
    {:ok, account} =
      Accounts.create_account(%{name: "benchmark-account-#{System.unique_integer([:positive])}"})

    # Create database using real Postgres credentials from Repo config
    repo_config = Map.new(Repo.config())

    {:ok, database} =
      Databases.create_db(account.id, %{
        name: "benchmark-db-#{System.unique_integer([:positive])}",
        hostname: repo_config[:hostname] || "localhost",
        port: repo_config[:port] || 5432,
        database: repo_config[:database] || "sequin_dev",
        username: repo_config[:username] || "postgres",
        password: repo_config[:password] || "postgres",
        pool_size: 2,
        ssl: false
      })

    # Create replication slot using domain function, skip validation since we're using existing slot
    {:ok, replication} =
      Replication.create_pg_replication(
        account.id,
        %{
          slot_name: @slot_name,
          publication_name: @pub_name,
          postgres_database_id: database.id
        },
        validate_slot?: false,
        validate_pub?: false
      )

    source_id = replication.id

    {replication, source_id}
  end

  defp setup_consumer(replication, partition_count) do
    {:ok, database} = Databases.get_db(replication.postgres_database_id)

    # Create benchmark consumer using domain function (creates database partition)
    {:ok, consumer} =
      Consumers.create_sink_consumer(database.account_id, %{
        name: "benchmark-consumer-#{System.unique_integer([:positive])}",
        status: :active,
        actions: [:insert, :update, :delete],
        replication_slot_id: replication.id,
        sink: %{type: :benchmark, partition_count: partition_count},
        source: %{include_table_oids: [16_384, 16_385, 16_386]}
      })

    consumer
  end

  # Build a mock flush_batch_fn for :reorder_buffer mode
  # This function mimics what BenchmarkPipeline does but at the ReorderBuffer level
  defp mock_flush_batch_fn(partition_count, checksum_owner_id) do
    fn _id, %Batch{messages: messages} ->
      # Messages are SlotProducer.Message with nested SlotProcessor.Message (in .message field)
      # Group by partition (using PK as group_id, same as BenchmarkSource)
      messages
      |> Enum.group_by(fn msg ->
        # Extract PK from message.ids (first element for single-column PK)
        group_id = msg.message.ids |> List.first() |> to_string()
        Stats.partition(group_id, partition_count)
      end)
      |> Enum.each(fn {partition, partition_msgs} ->
        # Sort within partition by (commit_lsn, commit_idx)
        partition_msgs
        |> Enum.sort_by(&{&1.message.commit_lsn, &1.message.commit_idx})
        |> Enum.each(fn msg ->
          Stats.message_received(
            checksum_owner_id,
            partition,
            msg.message.commit_lsn,
            msg.message.commit_idx,
            byte_size: msg.byte_size,
            created_at_us: extract_created_at(msg.message.fields)
          )
        end)
      end)

      :ok
    end
  end

  # Extract created_at from message fields (stored as microseconds since epoch)
  defp extract_created_at(fields) when is_list(fields) do
    case Enum.find(fields, fn f -> f.column_name == "created_at" end) do
      %{value: created_at} when is_integer(created_at) ->
        created_at

      %{value: created_at} when is_binary(created_at) ->
        case Integer.parse(created_at) do
          {int, ""} -> int
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp extract_created_at(_), do: nil

  defp capture_metrics(consumer_id, consumer_name) do
    %{
      messages_delivered: get_counter_value(:sequin_message_deliver_success_count, [consumer_id, consumer_name]),
      bytes_delivered: get_counter_value(:sequin_bytes_delivered_total, [consumer_id, consumer_name]),
      e2e_latency: get_histogram_stats(:sequin_benchmark_e2e_latency_us, [consumer_id]),
      delivery_latency: get_histogram_stats(:sequin_delivery_latency_us, [consumer_id, consumer_name, :ok])
    }
  end

  # Capture metrics from the Stats ETS module (for :reorder_buffer mode)
  defp capture_metrics_from_module(owner_id) do
    metrics = Stats.metrics(owner_id)
    checksums = Stats.checksums(owner_id)

    # Message count is sum of all partition counts from checksums
    message_count = checksums |> Map.values() |> Enum.map(&elem(&1, 1)) |> Enum.sum()

    %{
      messages_delivered: message_count,
      bytes_delivered: metrics.bytes,
      e2e_latency: %{count: metrics.latency_count, sum: metrics.latency_sum_us},
      # delivery_latency not applicable for reorder_buffer mode
      delivery_latency: %{count: 0, sum: 0}
    }
  end

  defp get_counter_value(name, labels) do
    case Prometheus.Metric.Counter.value(name: name, labels: labels) do
      :undefined -> 0
      value when is_number(value) -> value
      _ -> 0
    end
  end

  defp get_histogram_stats(name, labels) do
    case Prometheus.Metric.Histogram.value(name: name, labels: labels) do
      {buckets, sum} when is_list(buckets) ->
        # Buckets contain counts per bucket, sum them for total count
        count = Enum.sum(buckets)
        %{buckets: buckets, count: count, sum: sum}

      _ ->
        %{buckets: [], count: 0, sum: 0}
    end
  end

  defp print_results(
         start_metrics,
         end_metrics,
         duration_s,
         source_checksums,
         pipeline_checksums,
         source_tracked,
         pipeline_tracked
       ) do
    # Calculate deltas
    messages = end_metrics.messages_delivered - start_metrics.messages_delivered
    bytes = end_metrics.bytes_delivered - start_metrics.bytes_delivered

    messages_per_sec = Float.round(messages / duration_s, 1)
    bytes_per_sec = Float.round(bytes / duration_s, 1)
    mb_per_sec = Float.round(bytes_per_sec / 1_000_000, 2)

    announce("#{@bold}=== Benchmark Results ===#{@reset}", @cyan)
    IO.puts("Duration: #{Float.round(duration_s, 1)}s")
    IO.puts("")

    announce("Throughput:", @yellow)
    IO.puts("  Messages: #{format_number(messages)} (#{format_number(messages_per_sec)} msg/s)")
    IO.puts("  Bytes: #{format_bytes(bytes)} (#{mb_per_sec} MB/s)")
    IO.puts("")

    # Latency stats
    announce("Latency (handle_batch):", @yellow)
    print_latency_stats(start_metrics.delivery_latency, end_metrics.delivery_latency)
    IO.puts("")

    announce("E2E Latency (source -> sink):", @yellow)
    print_latency_stats(start_metrics.e2e_latency, end_metrics.e2e_latency)
    IO.puts("")

    # Verification
    announce("Verification:", @yellow)
    verify_checksums(source_checksums, pipeline_checksums)

    # Message tracking comparison
    IO.puts("")
    announce("Message Tracking:", @yellow)

    if Stats.track_messages?() do
      compare_tracked_messages(source_tracked, pipeline_tracked)
    else
      IO.puts("  Tracking disabled (set BENCHMARK_TRACK_MESSAGES=true to enable)")
    end
  end

  defp print_latency_stats(start_hist, end_hist) do
    count = end_hist.count - start_hist.count
    sum = end_hist.sum - start_hist.sum

    if count > 0 do
      avg_us = sum / count
      avg_ms = Float.round(avg_us / 1000, 2)

      # For percentiles, we'd need to compute from bucket deltas
      # For now, just show average
      IO.puts("  Samples: #{format_number(count)}")
      IO.puts("  Average: #{avg_ms}ms")
    else
      IO.puts("  No samples collected")
    end
  end

  defp verify_checksums(source_checksums, pipeline_checksums) do
    # Compare checksums
    source_partitions = source_checksums |> Map.keys() |> Enum.sort()
    pipeline_partitions = pipeline_checksums |> Map.keys() |> Enum.sort()

    if source_partitions == pipeline_partitions do
      mismatches =
        Enum.filter(source_partitions, fn partition ->
          source_checksums[partition] != pipeline_checksums[partition]
        end)

      if Enum.empty?(mismatches) do
        IO.puts("  #{@green}Checksums: PASS (all #{length(source_partitions)} partitions match)#{@reset}")
      else
        IO.puts("  #{@yellow}Checksums: PARTIAL (#{length(mismatches)} partitions differ)#{@reset}")

        Enum.each(mismatches, fn partition ->
          {src_checksum, src_count} = source_checksums[partition]
          {pipe_checksum, pipe_count} = pipeline_checksums[partition]

          IO.puts(
            "    Partition #{partition}: source={#{src_checksum}, #{src_count}} pipeline={#{pipe_checksum}, #{pipe_count}}"
          )
        end)
      end
    else
      IO.puts("  #{@yellow}WARNING: Partition mismatch#{@reset}")
      IO.puts("    Source partitions: #{inspect(source_partitions)}")
      IO.puts("    Pipeline partitions: #{inspect(pipeline_partitions)}")
    end
  end

  defp compare_tracked_messages(source_tracked, pipeline_tracked) do
    source_count = length(source_tracked)
    pipeline_count = length(pipeline_tracked)

    IO.puts("  Source generated: #{source_count} messages")
    IO.puts("  Pipeline received: #{pipeline_count} messages")

    # Convert to sets for comparison (just lsn, idx - ignore partition for now)
    source_set = MapSet.new(source_tracked, fn {lsn, idx, _partition} -> {lsn, idx} end)
    pipeline_set = MapSet.new(pipeline_tracked, fn {lsn, idx, _partition} -> {lsn, idx} end)

    missing = source_set |> MapSet.difference(pipeline_set) |> MapSet.to_list() |> Enum.sort()
    extra = pipeline_set |> MapSet.difference(source_set) |> MapSet.to_list() |> Enum.sort()

    if Enum.empty?(missing) and Enum.empty?(extra) do
      IO.puts("  #{@green}All messages delivered#{@reset}")

      # Compute checksums from sorted tracked messages (post-hoc verification)
      verify_post_hoc_checksums(source_tracked, pipeline_tracked)
    else
      if not Enum.empty?(missing) do
        IO.puts("  #{@yellow}Missing #{length(missing)} messages:#{@reset}")

        # Show first 10 missing messages
        missing
        |> Enum.take(10)
        |> Enum.each(fn {lsn, idx} ->
          IO.puts("    {lsn: #{lsn}, idx: #{idx}}")
        end)

        if length(missing) > 10 do
          IO.puts("    ... and #{length(missing) - 10} more")
        end
      end

      if not Enum.empty?(extra) do
        IO.puts("  #{@yellow}Extra #{length(extra)} messages in pipeline:#{@reset}")

        extra
        |> Enum.take(10)
        |> Enum.each(fn {lsn, idx} ->
          IO.puts("    {lsn: #{lsn}, idx: #{idx}}")
        end)

        if length(extra) > 10 do
          IO.puts("    ... and #{length(extra) - 10} more")
        end
      end
    end
  end

  # Compute checksums from tracked messages after sorting
  # This verifies that we received the SAME messages (regardless of order)
  defp verify_post_hoc_checksums(source_tracked, pipeline_tracked) do
    # Group by partition, sort by (lsn, idx), compute rolling checksum
    source_by_partition = Enum.group_by(source_tracked, fn {_lsn, _idx, partition} -> partition end)
    pipeline_by_partition = Enum.group_by(pipeline_tracked, fn {_lsn, _idx, partition} -> partition end)

    partitions = source_by_partition |> Map.keys() |> Enum.sort()

    results =
      Enum.map(partitions, fn partition ->
        source_msgs = source_by_partition |> Map.get(partition, []) |> Enum.sort_by(fn {lsn, idx, _} -> {lsn, idx} end)

        pipeline_msgs =
          pipeline_by_partition |> Map.get(partition, []) |> Enum.sort_by(fn {lsn, idx, _} -> {lsn, idx} end)

        # Compute checksums
        source_checksum = compute_rolling_checksum(source_msgs)
        pipeline_checksum = compute_rolling_checksum(pipeline_msgs)

        {partition, source_checksum, pipeline_checksum, source_checksum == pipeline_checksum}
      end)

    all_match = Enum.all?(results, fn {_, _, _, match} -> match end)

    if all_match do
      IO.puts("  #{@green}Post-hoc checksums: PASS (all #{length(partitions)} partitions match after sorting)#{@reset}")
    else
      mismatches = Enum.reject(results, fn {_, _, _, match} -> match end)
      IO.puts("  #{@yellow}Post-hoc checksums: #{length(mismatches)} partitions differ#{@reset}")

      mismatches
      |> Enum.take(3)
      |> Enum.each(fn {partition, src_cs, pipe_cs, _} ->
        IO.puts("    Partition #{partition}: source=#{src_cs} pipeline=#{pipe_cs}")
      end)
    end
  end

  defp compute_rolling_checksum(messages) do
    Enum.reduce(messages, 0, fn {lsn, idx, _partition}, prev_checksum ->
      :erlang.crc32(<<prev_checksum::32, lsn::64, idx::32>>)
    end)
  end

  defp parse_distribution(nil, default), do: default

  defp parse_distribution(str, _default) do
    str
    |> String.split(",")
    |> Enum.map(fn part ->
      [fraction_str, value_str] = String.split(part, ":")
      {fraction, ""} = Float.parse(fraction_str)
      {value, ""} = Integer.parse(value_str)
      {fraction, value}
    end)
  end

  defp format_number(num) when is_float(num), do: :erlang.float_to_binary(num, decimals: 1)

  defp format_number(num) when is_integer(num) do
    num
    |> Integer.to_string()
    |> String.reverse()
    |> String.to_charlist()
    |> Enum.chunk_every(3)
    |> Enum.join(",")
    |> String.reverse()
  end

  defp format_bytes(bytes) when bytes < 1000, do: "#{bytes} B"
  defp format_bytes(bytes) when bytes < 1_000_000, do: "#{Float.round(bytes / 1000, 1)} KB"
  defp format_bytes(bytes) when bytes < 1_000_000_000, do: "#{Float.round(bytes / 1_000_000, 1)} MB"
  defp format_bytes(bytes), do: "#{Float.round(bytes / 1_000_000_000, 2)} GB"

  defp announce(message, color) do
    IO.puts("#{color}#{message}#{@reset}")
  end
end
