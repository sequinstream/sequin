defmodule Sequin.Metrics.Prometheus do
  @moduledoc false

  # TODO: reporter_options.prometheus_type
  # [:peep, :packet, :sent]. Metadata contains %{size: packet_size}
  # [:peep, :packet, :error]. Metadata contains %{reason: reason}

  alias Sequin.Metrics.EctoBucket
  alias Sequin.Metrics.LatencyBucket
  alias Telemetry.Metrics

  @spec metrics() :: :ok
  def metrics do
    Metrics.last_value(:sequin_ingestion_latency_us,
      event_name: [:sequin, :ingestion_latency_us, :duration],
      tags: [:replication_slot_id, :slot_name],
      description: "The ingestion latency between Postgres and Sequin in microseconds."
    )

    Metrics.distribution(:sequin_internal_latency_us,
      event_name: [:sequin, :internal_latency_us, :duration],
      tags: [:consumer_id, :consumer_name],
      reporter_options: [peep_bucket_calculator: LatencyBucket],
      unit: false,
      description: "The internal processing latency in microseconds."
    )

    Metrics.distribution(:sequin_delivery_latency_us,
      event_name: [:sequin, :delivery_latency_us, :duration],
      tags: [:consumer_id, :consumer_name, :success],
      reporter_options: [peep_bucket_calculator: LatencyBucket],
      unit: false,
      description: "The delivery latency in microseconds."
    )

    Metrics.distribution(:sequin_post_delivery_latency_us,
      event_name: [:sequin, :post_delivery_latency_us, :duration],
      tags: [:consumer_id, :consumer_name],
      reporter_options: [peep_bucket_calculator: LatencyBucket],
      unit: false,
      description: "The post-delivery latency in microseconds."
    )

    Metrics.distribution(:sequin_oldest_message_age_ms,
      event_name: [:sequin, :oldest_message_age_ms, :duration],
      tags: [:consumer_id, :consumer_name],
      reporter_options: [peep_bucket_calculator: MessageAgeBucket],
      unit: false,
      description: "The approximate age of the oldest message in milliseconds."
    )

    Metrics.counter(:sequin_messages_ingested_count,
      event_name: [:sequin, :messages_ingested_count, :count],
      description: "Total number of messages ingested.",
      tags: [:replication_slot_id, :slot_name]
    )

    Metrics.counter(:sequin_message_deliver_attempt_count,
      event_name: [:sequin, :message_deliver_attempt_count, :count],
      description: "Total number of messages attempted for delivery.",
      tags: [:consumer_id, :consumer_name]
    )

    Metrics.counter(:sequin_message_deliver_success_count,
      event_name: [:sequin, :message_deliver_success_count, :count],
      description: "Total number of messages successfully delivered.",
      tags: [:consumer_id, :consumer_name]
    )

    Metrics.counter(:sequin_message_deliver_failure_count,
      event_name: [:sequin, :message_deliver_failure_count, :count],
      description: "Total number of messages that failed delivery.",
      tags: [:consumer_id, :consumer_name]
    )

    Metrics.counter(:sequin_messages_in_delivery,
      event_name: [:sequin, :messages_in_delivery, :count],
      tags: [:consumer_id, :consumer_name],
      description: "Approximate number of messages currently in delivery."
    )

    Metrics.counter(:sequin_messages_buffered,
      event_name: [:sequin, :messages_buffered, :count],
      tags: [:consumer_id, :consumer_name],
      description: "Approximate number of messages currently buffered."
    )

    Metrics.counter(:sequin_bytes_ingested_total,
      event_name: [:sequin, :bytes_ingested_total, :count],
      description: "Total number of bytes ingested.",
      tags: [:consumer_id, :consumer_name]
    )

    Metrics.counter(:sequin_bytes_delivered_total,
      event_name: [:sequin, :bytes_delivered_total, :count],
      description: "Total number of bytes delivered.",
      tags: [:consumer_id, :consumer_name]
    )

    Metrics.counter(:sequin_messages_in_redelivery,
      event_name: [:sequin, :messages_in_redelivery, :count],
      tags: [:consumer_id, :consumer_name],
      description: "Number of messages currently in re-delivery."
    )

    Metrics.last_value(:sequin_buffer_memory_utilization_mb,
      event_name: [:sequin, :buffer_memory_utilization_mb, :mb],
      tags: [:consumer_id, :consumer_name],
      description: "Buffer memory utilization in megabytes."
    )

    Metrics.last_value(:sequin_buffer_memory_utilization_percent,
      event_name: [:sequin, :buffer_memory_utilization_percent, :percent],
      tags: [:consumer_id, :consumer_name],
      description: "Buffer memory utilization as a percentage."
    )

    Metrics.last_value(:sequin_ingestion_saturation_percent,
      event_name: [:sequin, :ingestion_saturation_percent, :percent],
      tags: [:consumer_id, :consumer_name],
      description: "Ingestion saturation as a percentage."
    )

    Metrics.last_value(:sequin_processing_saturation_percent,
      event_name: [:sequin, :processing_saturation_percent, :percent],
      tags: [:consumer_id, :consumer_name],
      description: "Processing saturation as a percentage."
    )

    Metrics.last_value(:sequin_delivery_saturation_percent,
      event_name: [:sequin, :delivery_saturation_percent, :percent],
      tags: [:consumer_id, :consumer_name],
      description: "Delivery saturation as a percentage."
    )

    ## Ecto
    Metrics.distribution(:sequin_repo_query_query,
      event_name: [:sequin, :repo, :query, :query_time],
      reporter_options: [peep_bucket_calculator: EctoBucket],
      tags: [:query],
      description: "Ecto query time: query.",
      unit: :milliseconds
    )

    Metrics.distribution(:sequin_repo_query_idle,
      event_name: [:sequin, :repo, :query, :idle_time],
      reporter_options: [peep_bucket_calculator: EctoBucket],
      tags: [:query],
      description: "Ecto query time: idle.",
      unit: :milliseconds
    )

    Metrics.distribution(:sequin_repo_query_queue,
      event_name: [:sequin, :repo, :query, :queue_time],
      reporter_options: [peep_bucket_calculator: EctoBucket],
      tags: [:query],
      description: "Ecto query time: queue.",
      unit: :milliseconds
    )

    Metrics.distribution(:sequin_repo_query_decode,
      event_name: [:sequin, :repo, :query, :decode_time],
      reporter_options: [peep_bucket_calculator: EctoBucket],
      tags: [:query],
      description: "Ecto query time: decode.",
      unit: :milliseconds
    )

    Metrics.distribution(:sequin_repo_query_total,
      event_name: [:sequin, :repo, :query, :total_time],
      reporter_options: [peep_bucket_calculator: EctoBucket],
      tags: [:query],
      description: "Ecto query time: total.",
      unit: :milliseconds
    )

    :ok
  end

  # no catchall - :telemetry prints error and removes handler in case of no match

  @spec increment_message_deliver_attempt(consumer_id :: String.t(), consumer_name :: String.t(), count :: number()) ::
          :ok
  def increment_message_deliver_attempt(consumer_id, consumer_name, count \\ 1) do
    :telemetry.execute([:sequin, :message_deliver_attempt_count], %{
      count: count,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  @spec increment_message_deliver_success(consumer_id :: String.t(), consumer_name :: String.t(), count :: number()) ::
          :ok
  def increment_message_deliver_success(consumer_id, consumer_name, count \\ 1) do
    :telemetry.execute([:sequin, :message_deliver_success_count], %{
      count: count,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  @spec increment_message_deliver_failure(consumer_id :: String.t(), consumer_name :: String.t(), count :: number()) ::
          :ok
  def increment_message_deliver_failure(consumer_id, consumer_name, count \\ 1) do
    :telemetry.execute([:sequin, :message_deliver_failure_count], %{
      count: count,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  @spec observe_messages_ingested_latency(consumer_id :: String.t(), consumer_name :: String.t(), latency_ms :: number()) ::
          :ok
  def observe_messages_ingested_latency(consumer_id, consumer_name, latency_ms) do
    :telemetry.execute([:sequin, :messages_ingested_latency_ms], %{
      latency_ms: latency_ms,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  @spec observe_ingestion_latency(replication_slot_id :: String.t(), slot_name :: String.t(), latency_us :: number()) ::
          :ok
  def observe_ingestion_latency(replication_slot_id, slot_name, latency_us) do
    :telemetry.execute([:sequin, :ingestion_latency_us], %{
      duration: latency_us,
      replication_slot_id: replication_slot_id,
      slot_name: slot_name
    })
  end

  @spec observe_internal_latency(consumer_id :: String.t(), consumer_name :: String.t(), latency_us :: number()) :: :ok
  def observe_internal_latency(consumer_id, consumer_name, latency_us) do
    :telemetry.execute([:sequin, :internal_latency_us], %{
      duration: latency_us,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  @spec observe_delivery_latency(
          consumer_id :: String.t(),
          consumer_name :: String.t(),
          success :: :ok | :error,
          latency_us :: number()
        ) :: :ok
  def observe_delivery_latency(consumer_id, consumer_name, success, latency_us) do
    validate_success(success)

    :telemetry.execute([:sequin, :delivery_latency_us], %{
      duration: latency_us,
      consumer_id: consumer_id,
      consumer_name: consumer_name,
      success: success
    })
  end

  @spec observe_post_delivery_latency(consumer_id :: String.t(), consumer_name :: String.t(), latency_us :: number()) ::
          :ok
  def observe_post_delivery_latency(consumer_id, consumer_name, latency_us) do
    :telemetry.execute([:sequin, :post_delivery_latency_us], %{
      duration: latency_us,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  @spec observe_oldest_message_age(consumer_id :: String.t(), consumer_name :: String.t(), age_ms :: number()) :: :ok
  def observe_oldest_message_age(consumer_id, consumer_name, age_ms) do
    :telemetry.execute([:sequin, :oldest_message_age_ms], %{
      age_ms: age_ms,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  @spec increment_messages_ingested(replication_slot_id :: String.t(), slot_name :: String.t(), count :: number()) :: :ok
  def increment_messages_ingested(replication_slot_id, slot_name, count \\ 1) do
    :telemetry.execute([:sequin, :messages_ingested_count], %{
      count: count,
      replication_slot_id: replication_slot_id,
      slot_name: slot_name
    })
  end

  @spec set_messages_in_delivery(consumer_id :: String.t(), consumer_name :: String.t(), count :: number()) :: :ok
  def set_messages_in_delivery(consumer_id, consumer_name, count) do
    :telemetry.execute([:sequin, :messages_in_delivery], %{
      count: count,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  @spec set_messages_buffered(consumer_id :: String.t(), consumer_name :: String.t(), count :: number()) :: :ok
  def set_messages_buffered(consumer_id, consumer_name, count) do
    :telemetry.execute([:sequin, :messages_buffered], %{
      count: count,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  @spec increment_bytes_ingested(consumer_id :: String.t(), consumer_name :: String.t(), bytes :: number()) :: :ok
  def increment_bytes_ingested(consumer_id, consumer_name, bytes) do
    :telemetry.execute([:sequin, :bytes_ingested_total], %{
      count: bytes,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  @spec increment_bytes_delivered(consumer_id :: String.t(), consumer_name :: String.t(), bytes :: number()) :: :ok
  def increment_bytes_delivered(consumer_id, consumer_name, bytes) do
    :telemetry.execute([:sequin, :bytes_delivered_total], %{
      count: bytes,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  @spec set_messages_in_redelivery(consumer_id :: String.t(), consumer_name :: String.t(), count :: number()) :: :ok
  def set_messages_in_redelivery(consumer_id, consumer_name, count) do
    :telemetry.execute([:sequin, :messages_in_redelivery], %{
      count: count,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  @spec set_buffer_memory_utilization_mb(consumer_id :: String.t(), consumer_name :: String.t(), mb :: number()) :: :ok
  def set_buffer_memory_utilization_mb(consumer_id, consumer_name, mb) do
    :telemetry.execute([:sequin, :buffer_memory_utilization_mb], %{
      mb: mb,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  @spec set_buffer_memory_utilization_percent(consumer_id :: String.t(), consumer_name :: String.t(), percent :: number()) ::
          :ok
  def set_buffer_memory_utilization_percent(consumer_id, consumer_name, percent) do
    :telemetry.execute([:sequin, :buffer_memory_utilization_percent], %{
      percent: percent,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  @spec set_ingestion_saturation(consumer_id :: String.t(), consumer_name :: String.t(), percent :: number()) :: :ok
  def set_ingestion_saturation(consumer_id, consumer_name, percent) do
    :telemetry.execute([:sequin, :ingestion_saturation_percent], %{
      percent: percent,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  @spec set_processing_saturation(consumer_id :: String.t(), consumer_name :: String.t(), percent :: number()) :: :ok
  def set_processing_saturation(consumer_id, consumer_name, percent) do
    :telemetry.execute([:sequin, :processing_saturation_percent], %{
      percent: percent,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  @spec set_delivery_saturation(consumer_id :: String.t(), consumer_name :: String.t(), percent :: number()) :: :ok
  def set_delivery_saturation(consumer_id, consumer_name, percent) do
    :telemetry.execute([:sequin, :delivery_saturation_percent], %{
      percent: percent,
      consumer_id: consumer_id,
      consumer_name: consumer_name
    })
  end

  defp validate_success(:ok), do: :ok
  defp validate_success(:error), do: :ok
end
