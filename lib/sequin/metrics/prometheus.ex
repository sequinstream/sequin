defmodule Sequin.Prometheus do
  @moduledoc false
  use Prometheus.Metric

  @spec setup() :: :ok
  def setup do
    Histogram.new(
      name: :sequin_messages_ingested_latency_ms,
      labels: [:consumer_id],
      buckets: [100, 300, 500, 750, 1000],
      duration_unit: :milliseconds,
      help: "The latency of messages being ingested from the replication slot."
    )

    Histogram.new(
      name: :sequin_replication_lag_ms,
      labels: [:consumer_id],
      buckets: [10, 50, 100, 250, 500, 1000, 2500, 5000, 10_000, 100_000, 1_000_000],
      duration_unit: :milliseconds,
      help: "The replication lag in milliseconds."
    )

    Histogram.new(
      name: :sequin_internal_latency_ms,
      labels: [:consumer_id],
      buckets: [10, 50, 100, 250, 500, 1000, 2500, 5000, 10_000],
      duration_unit: :milliseconds,
      help: "The internal processing latency in milliseconds."
    )

    Histogram.new(
      name: :sequin_delivery_latency_ms,
      labels: [:consumer_id],
      buckets: [10, 50, 100, 250, 500, 1000, 2500, 5000, 10_000],
      duration_unit: :milliseconds,
      help: "The delivery latency in milliseconds."
    )

    Histogram.new(
      name: :sequin_post_delivery_latency_ms,
      labels: [:consumer_id],
      buckets: [10, 50, 100, 250, 500, 1000, 2500, 5000, 10_000],
      duration_unit: :milliseconds,
      help: "The post-delivery latency in milliseconds."
    )

    Histogram.new(
      name: :sequin_oldest_message_age_ms,
      labels: [:consumer_id],
      buckets: [1000, 5000, 10_000, 30_000, 60_000, 300_000, 600_000, 1_800_000, 3_600_000],
      duration_unit: :milliseconds,
      help: "The approximate age of the oldest message in milliseconds."
    )

    Counter.declare(
      name: :sequin_messages_ingested_total,
      help: "Total number of messages ingested.",
      labels: [:replication_slot_id]
    )

    Counter.declare(
      name: :sequin_message_deliver_attempt,
      help: "Total number of messages attempted for delivery.",
      labels: [:consumer_id]
    )

    Counter.declare(
      name: :sequin_message_deliver_success,
      help: "Total number of messages successfully delivered.",
      labels: [:consumer_id]
    )

    Counter.declare(
      name: :sequin_message_deliver_failure,
      help: "Total number of messages that failed delivery.",
      labels: [:consumer_id]
    )

    Gauge.new(
      name: :sequin_messages_in_delivery,
      labels: [:consumer_id],
      help: "Approximate number of messages currently in delivery."
    )

    Gauge.new(
      name: :sequin_messages_buffered,
      labels: [:consumer_id],
      help: "Approximate number of messages currently buffered."
    )

    Counter.declare(
      name: :sequin_bytes_ingested_total,
      help: "Total number of bytes ingested.",
      labels: [:consumer_id]
    )

    Counter.declare(
      name: :sequin_bytes_delivered_total,
      help: "Total number of bytes delivered.",
      labels: [:consumer_id]
    )

    Counter.declare(
      name: :sequin_messages_failed_per_second,
      help: "Number of messages that failed delivery per second.",
      labels: [:consumer_id]
    )

    Gauge.new(
      name: :sequin_messages_in_redelivery,
      labels: [:consumer_id],
      help: "Number of messages currently in re-delivery."
    )

    Gauge.new(
      name: :sequin_buffer_memory_utilization_mb,
      labels: [:consumer_id],
      help: "Buffer memory utilization in megabytes."
    )

    Gauge.new(
      name: :sequin_buffer_memory_utilization_percent,
      labels: [:consumer_id],
      help: "Buffer memory utilization as a percentage."
    )

    Gauge.new(
      name: :sequin_ingestion_saturation_percent,
      labels: [:consumer_id],
      help: "Ingestion saturation as a percentage."
    )

    Gauge.new(
      name: :sequin_processing_saturation_percent,
      labels: [:consumer_id],
      help: "Processing saturation as a percentage."
    )

    Gauge.new(
      name: :sequin_delivery_saturation_percent,
      labels: [:consumer_id],
      help: "Delivery saturation as a percentage."
    )

    ## Ecto
    :ok = :telemetry.attach("sequin-repo-query", [:sequin, :repo, :query], &Sequin.Prometheus.ecto_event/4, %{})

    Histogram.new(
      name: :sequin_repo_query_query,
      buckets: [1, 10, 100, 1000, 10_000],
      labels: [:query],
      help: "Ecto query time: query.",
      duration_unit: :milliseconds
    )

    Histogram.new(
      name: :sequin_repo_query_idle,
      buckets: [1, 10, 100, 1000, 10_000],
      labels: [:query],
      help: "Ecto query time: idle.",
      duration_unit: :milliseconds
    )

    Histogram.new(
      name: :sequin_repo_query_queue,
      buckets: [1, 10, 100, 1000, 10_000],
      labels: [:query],
      help: "Ecto query time: queue.",
      duration_unit: :milliseconds
    )

    Histogram.new(
      name: :sequin_repo_query_decode,
      buckets: [1, 10, 100, 1000, 10_000],
      labels: [:query],
      help: "Ecto query time: decode.",
      duration_unit: :milliseconds
    )

    Histogram.new(
      name: :sequin_repo_query_total,
      buckets: [1, 10, 100, 1000, 10_000],
      labels: [:query],
      help: "Ecto query time: total.",
      duration_unit: :milliseconds
    )
  end

  def ecto_event([:sequin, :repo, :query], measurements, metadata, _config) do
    # IO.inspect(measurements, label: "measurements")
    lbls = [String.slice(metadata.query, 0, 100)]

    Histogram.observe([name: :sequin_repo_query_query, labels: lbls], measurements.query_time)
    Histogram.observe([name: :sequin_repo_query_total, labels: lbls], measurements.total_time)

    case Map.fetch(metadata, :idle_time) do
      {:ok, t} -> Histogram.observe([name: :sequin_repo_query_idle, labels: lbls], t)
      _ -> nil
    end

    case Map.fetch(metadata, :decode_time) do
      {:ok, t} -> Histogram.observe([name: :sequin_repo_query_decode, labels: lbls], t)
      _ -> nil
    end

    case Map.fetch(metadata, :queue_time) do
      {:ok, t} -> Histogram.observe([name: :sequin_repo_query_queue, labels: lbls], t)
      _ -> nil
    end
  end

  # no catchall - :telemetry prints error and removes handler in case of no match

  @spec increment_message_deliver_attempt(consumer_id :: String.t(), count :: number()) :: :ok
  def increment_message_deliver_attempt(consumer_id, count \\ 1) do
    Counter.inc(
      name: :sequin_message_deliver_attempt,
      labels: [consumer_id],
      count: count
    )
  end

  @spec increment_message_deliver_success(consumer_id :: String.t(), count :: number()) :: :ok
  def increment_message_deliver_success(consumer_id, count \\ 1) do
    Counter.inc(
      name: :sequin_message_deliver_success,
      labels: [consumer_id],
      count: count
    )
  end

  @spec increment_message_deliver_failure(consumer_id :: String.t(), count :: number()) :: :ok
  def increment_message_deliver_failure(consumer_id, count \\ 1) do
    Counter.inc(
      name: :sequin_message_deliver_failure,
      labels: [consumer_id],
      count: count
    )
  end

  @spec observe_messages_ingested_latency(consumer_id :: String.t(), latency_ms :: number()) :: :ok
  def observe_messages_ingested_latency(consumer_id, latency_ms) do
    Histogram.observe(
      name: :sequin_messages_ingested_latency_ms,
      labels: [consumer_id],
      value: latency_ms
    )
  end

  @spec observe_replication_lag(consumer_id :: String.t(), lag_ms :: number()) :: :ok
  def observe_replication_lag(consumer_id, lag_ms) do
    Histogram.observe([name: :sequin_replication_lag_ms, labels: [consumer_id]], lag_ms)
  end

  @spec observe_internal_latency(consumer_id :: String.t(), latency_ms :: number()) :: :ok
  def observe_internal_latency(consumer_id, latency_ms) do
    Histogram.observe([name: :sequin_internal_latency_ms, labels: [consumer_id]], latency_ms)
  end

  @spec observe_delivery_latency(consumer_id :: String.t(), latency_ms :: number()) :: :ok
  def observe_delivery_latency(consumer_id, latency_ms) do
    Histogram.observe([name: :sequin_delivery_latency_ms, labels: [consumer_id]], latency_ms)
  end

  @spec observe_post_delivery_latency(consumer_id :: String.t(), latency_ms :: number()) :: :ok
  def observe_post_delivery_latency(consumer_id, latency_ms) do
    Histogram.observe([name: :sequin_post_delivery_latency_ms, labels: [consumer_id]], latency_ms)
  end

  @spec observe_oldest_message_age(consumer_id :: String.t(), age_ms :: number()) :: :ok
  def observe_oldest_message_age(consumer_id, age_ms) do
    Histogram.observe([name: :sequin_oldest_message_age_ms, labels: [consumer_id]], age_ms)
  end

  @spec increment_messages_ingested(replication_slot_id :: String.t(), count :: number()) :: :ok
  def increment_messages_ingested(replication_slot_id, count \\ 1) do
    Counter.inc([name: :sequin_messages_ingested_total, labels: [replication_slot_id]], count)
  end

  @spec set_messages_in_delivery(consumer_id :: String.t(), count :: number()) :: :ok
  def set_messages_in_delivery(consumer_id, count) do
    Gauge.set([name: :sequin_messages_in_delivery, labels: [consumer_id]], count)
  end

  @spec set_messages_buffered(consumer_id :: String.t(), count :: number()) :: :ok
  def set_messages_buffered(consumer_id, count) do
    Gauge.set([name: :sequin_messages_buffered, labels: [consumer_id]], count)
  end

  @spec increment_bytes_ingested(consumer_id :: String.t(), bytes :: number()) :: :ok
  def increment_bytes_ingested(consumer_id, bytes) do
    Counter.inc(
      name: :sequin_bytes_ingested_total,
      labels: [consumer_id],
      count: bytes
    )
  end

  @spec increment_bytes_delivered(consumer_id :: String.t(), bytes :: number()) :: :ok
  def increment_bytes_delivered(consumer_id, bytes) do
    Counter.inc(
      name: :sequin_bytes_delivered_total,
      labels: [consumer_id],
      count: bytes
    )
  end

  @spec increment_messages_failed_per_second(consumer_id :: String.t(), count :: number()) :: :ok
  def increment_messages_failed_per_second(consumer_id, count \\ 1) do
    Counter.inc(
      name: :sequin_messages_failed_per_second,
      labels: [consumer_id],
      count: count
    )
  end

  @spec set_messages_in_redelivery(consumer_id :: String.t(), count :: number()) :: :ok
  def set_messages_in_redelivery(consumer_id, count) do
    Gauge.set([name: :sequin_messages_in_redelivery, labels: [consumer_id]], count)
  end

  @spec set_buffer_memory_utilization_mb(consumer_id :: String.t(), mb :: number()) :: :ok
  def set_buffer_memory_utilization_mb(consumer_id, mb) do
    Gauge.set([name: :sequin_buffer_memory_utilization_mb, labels: [consumer_id]], mb)
  end

  @spec set_buffer_memory_utilization_percent(consumer_id :: String.t(), percent :: number()) :: :ok
  def set_buffer_memory_utilization_percent(consumer_id, percent) do
    Gauge.set([name: :sequin_buffer_memory_utilization_percent, labels: [consumer_id]], percent)
  end

  @spec set_ingestion_saturation(consumer_id :: String.t(), percent :: number()) :: :ok
  def set_ingestion_saturation(consumer_id, percent) do
    Gauge.set([name: :sequin_ingestion_saturation_percent, labels: [consumer_id]], percent)
  end

  @spec set_processing_saturation(consumer_id :: String.t(), percent :: number()) :: :ok
  def set_processing_saturation(consumer_id, percent) do
    Gauge.set([name: :sequin_processing_saturation_percent, labels: [consumer_id]], percent)
  end

  @spec set_delivery_saturation(consumer_id :: String.t(), percent :: number()) :: :ok
  def set_delivery_saturation(consumer_id, percent) do
    Gauge.set([name: :sequin_delivery_saturation_percent, labels: [consumer_id]], percent)
  end
end
