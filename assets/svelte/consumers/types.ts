import type { Table } from "../databases/types";
import type { Function } from "../functions/types";

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
  type:
    | "http_push"
    | "sqs"
    | "sns"
    | "kinesis"
    | "redis_stream"
    | "kafka"
    | "sequin_stream"
    | "gcp_pubsub"
    | "nats"
    | "rabbitmq"
    | "typesense"
    | "elasticsearch"
    | "redis_string";
  name: string;
  annotations: Record<string, boolean>;
  status: "active" | "paused" | "disabled";
  message_kind: string;
  ack_wait_ms: number;
  max_ack_pending: number;
  max_deliver: number;
  max_waiting: number;
  inserted_at: string;
  updated_at: string;
  sequence: {
    table_name: string;
    table_schema: string;
    column_filters: Array<{
      column: string;
      operator: string;
      value: any;
      is_jsonb: boolean;
      jsonb_path: string;
    }>;
  };
  routing_id: string | null;
  routing: Function | null;
  filter_id: string | null;
  filter: Function | null;
  table: Table;
  postgres_database: {
    id: string;
    name: string;
    pg_major_version: number;
  };
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
    access_key_id: string;
    secret_access_key: string;
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
  routing_mode?: "static" | "dynamic";
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
    event_hub_name: string;
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
    exchange: string;
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
    project_id: string;
    topic_id: string;
    connection_id: string;
    use_emulator: boolean;
    emulator_base_url: string;
  };
};

// Typesense specific sink
export type TypesenseConsumer = BaseConsumer & {
  sink: {
    type: "typesense";
    endpoint_url: string;
    collection_name: string;
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
    auth_type: "api_key" | "basic" | "bearer";
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
