// Base consumer type with shared properties
export type BaseConsumer = {
  id: string;
  type: "http_push" | "sqs";
  name: string;
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

// HTTP Push specific destination
export type HttpPushConsumer = BaseConsumer & {
  destination: {
    type: "http_push";
    http_endpoint: {
      id: string;
      url: string;
    };
    http_endpoint_path: string;
  };
};

// SQS specific destination
export type SqsConsumer = BaseConsumer & {
  destination: {
    type: "sqs";
    queue_url: string;
    region: string;
    is_fifo: boolean;
  };
};

// Redis specific destination
export type RedisConsumer = BaseConsumer & {
  destination: {
    type: "redis";
    host: string;
    port: number;
    streamKey: string;
    database: number;
    tls: boolean;
    url: string;
  };
};

// Union type for all consumer types
export type Consumer = HttpPushConsumer | SqsConsumer | RedisConsumer;
