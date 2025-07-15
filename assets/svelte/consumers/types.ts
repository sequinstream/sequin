import type { Database, Table } from "../databases/types";
import type { Function } from "../functions/types";

export type Source = {
  include_schemas: string[] | null;
  exclude_schemas: string[] | null;
  include_table_oids: number[] | null;
  exclude_table_oids: number[] | null;
};

export type Backfill = {
  id: string;
  state: "active" | "completed" | "failed" | "canceled";
  table_name: string;
  rows_initial_count: number | null;
  rows_processed_count: number;
  rows_ingested_count: number;
  completed_at: string | null;
  canceled_at: string | null;
  inserted_at: string;
  updated_at: string;
  progress: number | null;
};

// Base consumer type with shared properties
export type BaseConsumer = {
  id: string;
  type: SinkType;
  name: string;
  annotations: Record<string, boolean>;
  status: "active" | "paused" | "disabled";
  message_kind: string;
  message_grouping: boolean;
  ack_wait_ms: number;
  max_ack_pending: number;
  max_deliver: number;
  max_waiting: number;
  inserted_at: string;
  updated_at: string;
  source: Source;
  tables_included_in_source: Table[];
  routing_id: string | null;
  routing: Function | null;
  enrichment_id: string | null;
  enrichment: Function | null;
  routing_mode?: "static" | "dynamic";
  filter_id: string | null;
  filter: Function | null;
  database: Database;
  active_backfills: Backfill[];
  health: any;
  href: string;
  group_column_names: string[];
  batch_size: number;
};

// HTTP Push specific sink
export type HttpPushConsumer = BaseConsumer & {
  sink: {
    type: "http_push";
    http_endpoint: {
      id: string;
      url: string;
    };
    http_endpoint_path: string;
  };
};

// SQS specific sink
export type SqsConsumer = BaseConsumer & {
  sink: {
    type: "sqs";
    queue_url: string;
    region: string;
    is_fifo: boolean;
  };
};

// Kinesis specific sink
export type KinesisConsumer = BaseConsumer & {
  sink: {
    type: "kinesis";
    stream_arn: string;
    region: string;
    access_key_id: string;
    secret_access_key: string;
  };
};

// S2 specific sink
export type S2Consumer = BaseConsumer & {
  sink: {
    type: "s2";
    basin: string;
    stream: string;
    access_token: string;
    dashboard_url: string;
  };
};

// Redis specific sink
export type RedisStreamConsumer = BaseConsumer & {
  sink: {
    type: "redis_stream";
    host: string;
    port: number;
    streamKey: string;
    database: number;
    tls: boolean;
    url: string;
  };
};

// RedisString specific sink
export type RedisStringConsumer = BaseConsumer & {
  sink: {
    type: "redis_string";
    host: string;
    port: number;
    database: number;
    tls: boolean;
    url: string;
    expireMs: number | null;
  };
};

// NATS specific sink
export type NatsConsumer = BaseConsumer & {
  sink: {
    type: "nats";
    host: string;
    port: number;
    subject: string;
    username: string;
    password: string;
    jwt: string;
    nkey_seed: string;
    tls: boolean;
  };
};

// Azure Event Hub specific sink
export type AzureEventHubConsumer = BaseConsumer & {
  sink: {
    type: "azure_event_hub";
    namespace: string;
    event_hub_name: string | null;
    shared_access_key_name: string;
    shared_access_key: string;
  };
};

// RabbitMQ specific sink
export type RabbitMqConsumer = BaseConsumer & {
  sink: {
    type: "rabbitmq";
    host: string;
    port: number;
    exchange: string | null;
    headers: Record<string, string> | null;
    topic: string;
    username: string;
    password?: string;
    virtual_host: string;
    tls: boolean;
  };
};

// Kafka specific sink
export type KafkaConsumer = BaseConsumer & {
  sink: {
    type: "kafka";
    url: string;
    hosts: string;
    username: string;
    password: string;
    tls: boolean;
    topic: string;
    sasl_mechanism: null | "plain" | "scram_sha_256" | "scram_sha_512";
  };
};

// Sequin Stream specific sink
export type SequinStreamConsumer = BaseConsumer & {
  sink: {
    type: "sequin_stream";
  };
};

// Gcp Pubsub specific sink
export type GcpPubsubConsumer = BaseConsumer & {
  sink: {
    type: "gcp_pubsub";
    elasticsearch;
    project_id: string;
    topic_id: string;
    use_emulator: boolean;
    emulator_base_url?: string;
  };
};

// Typesense specific sink
export type TypesenseConsumer = BaseConsumer & {
  sink: {
    type: "typesense";
    endpoint_url: string;
    api_key: string;
    collection_name: string;
    batch_size: number;
    timeout_seconds: number;
  };
};

// Meilisearch specific sink
export type MeilisearchConsumer = BaseConsumer & {
  sink: {
    type: "meilisearch";
    endpoint_url: string;
    index_name: string;
    api_key: string;
    primary_key: string;
    batch_size: number;
    timeout_seconds: number;
  };
};

// SNS specific sink
export type SnsConsumer = BaseConsumer & {
  sink: {
    type: "sns";
    topic_arn: string;
    region: string;
    is_fifo: boolean;
  };
};

// Elasticsearch specific sink
export type ElasticsearchConsumer = BaseConsumer & {
  sink: {
    type: "elasticsearch";
    endpoint_url: string;
    index_name: string;
    auth_type: "none" | "api_key" | "basic" | "bearer";
    auth_value: string;
    batch_size: number;
    timeout_seconds: number;
  };
};

// Union type for all consumer types
export type Consumer =
  | HttpPushConsumer
  | SqsConsumer
  | KinesisConsumer
  | S2Consumer
  | RedisStreamConsumer
  | KafkaConsumer
  | SequinStreamConsumer
  | GcpPubsubConsumer
  | NatsConsumer
  | AzureEventHubConsumer
  | RabbitMqConsumer
  | TypesenseConsumer
  | SnsConsumer
  | ElasticsearchConsumer
  | RedisStringConsumer;

export const SinkTypeValues = [
  "http_push",
  "sqs",
  "sns",
  "kinesis",
  "redis_stream",
  "kafka",
  "sequin_stream",
  "gcp_pubsub",
  "elasticsearch",
  "nats",
  "rabbitmq",
  "typesense",
  "meilisearch",
  "elasticsearch",
  "redis_string",
] as const;

export type SinkType = (typeof SinkTypeValues)[number];

export const RoutedSinkTypeValues = [
  "http_push",
  "redis_string",
  "redis_stream",
  "nats",
  "kafka",
  "gcp_pubsub",
  "typesense",
  "meilisearch",
  "elasticsearch",
  "azure_event_hub",
  "rabbitmq",
  "s2",
  "sqs",
  "sns",
  "kinesis",
] as const;

export type RoutedSinkType = (typeof RoutedSinkTypeValues)[number];
