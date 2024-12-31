// Base consumer type with shared properties
export type BaseConsumer = {
  id: string;
  type:
    | "http_push"
    | "sqs"
    | "redis"
    | "kafka"
    | "sequin_stream"
    | "gcp_pubsub"
    | "nats"
    | "rabbitmq";
  name: string;
  annotations: Record<string, boolean>;
  status: string;
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
  postgres_database: {
    id: string;
    name: string;
  };
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

// Redis specific sink
export type RedisConsumer = BaseConsumer & {
  sink: {
    type: "redis";
    host: string;
    port: number;
    streamKey: string;
    database: number;
    tls: boolean;
    url: string;
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

// Union type for all consumer types
export type Consumer =
  | HttpPushConsumer
  | SqsConsumer
  | RedisConsumer
  | KafkaConsumer
  | SequinStreamConsumer
  | GcpPubsubConsumer
  | NatsConsumer
  | RabbitMqConsumer;
