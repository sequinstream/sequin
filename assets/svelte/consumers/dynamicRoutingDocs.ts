import type { RoutedSinkType } from "./types";

type RoutedSinkDocs = {
  fields: {
    [key: string]: {
      description: string;
      // The value of the field when static routing is enabled (staticFormField can override this)
      staticValue: string;
      // If you set this field it will attempt to sync the field from the sink form
      staticFormField?: string;
      // The default value for the field when dynamic routing is enabled
      dynamicDefault: string;
    };
  };
};

export const routedSinkDocs: Record<RoutedSinkType, RoutedSinkDocs> = {
  http_push: {
    fields: {
      method: {
        description: "HTTP method (GET, POST, PUT, etc.)",
        staticValue: "POST",
        dynamicDefault: "POST",
      },
      endpoint_path: {
        description: "Path portion of the URL, it is appended to the base URL",
        staticValue: "<empty>",
        dynamicDefault: "<empty>",
        staticFormField: "httpEndpointPath",
      },
      headers: {
        description:
          "Map of key value pairs, including headers and encrypted headers from the HTTP endpoint",
        staticValue: "<see-http-endpoint>",
        dynamicDefault: "<see-http-endpoint>",
      },
    },
  },
  redis_string: {
    fields: {
      key: {
        description: "The Redis key to store the message in",
        staticValue: "sequin:<table-name>:<primary-keys>",
        dynamicDefault: "sequin:<table-name>:<primary-keys>",
      },
      action: {
        description: "The Redis action to perform",
        staticValue: "'set' for insert, update, and read, 'del' for delete",
        dynamicDefault: "'set' for insert, update, and read, 'del' for delete",
      },
      expire_ms: {
        description:
          "The expiration time in milliseconds, if nil the key will not expire",
        staticValue: "nil",
        dynamicDefault: "nil",
        staticFormField: "expireMs",
      },
    },
  },
  nats: {
    fields: {
      subject: {
        description: "The NATS subject to publish messages to",
        staticValue:
          "sequin.<database_name>.<table_schema>.<table_name>.<action>",
        dynamicDefault:
          "sequin.<database_name>.<table_schema>.<table_name>.<action>",
      },
      headers: {
        description: "Map of key value pairs",
        staticValue: '%{"Idempotency-Key" => <idempotency-key>}',
        dynamicDefault: '%{"Idempotency-Key" => <idempotency-key>}',
      },
    },
  },
  kafka: {
    fields: {
      topic: {
        description: "Kafka topic to publish to",
        staticValue: "<empty>",
        staticFormField: "topic",
        dynamicDefault: "sequin.<database_name>.<table_schema>.<table_name>",
      },
    },
  },
  gcp_pubsub: {
    fields: {
      topic_id: {
        description: "GCP PubSub topic ID to publish to",
        staticValue: "<empty>",
        staticFormField: "topic_id",
        dynamicDefault: "sequin.<table_schema>.<table_name>",
      },
    },
  },
  typesense: {
    fields: {
      action: {
        description: "Typesense action to perform",
        staticValue:
          "'index' for insert, update, and read, 'delete' for delete",
        dynamicDefault:
          "'index' for insert, update, and read, 'delete' for delete",
      },
      collection_name: {
        description: "Typesense collection name",
        staticValue: "<empty>",
        staticFormField: "collection_name",
        dynamicDefault: "sequin.<table_schema>.<table_name>",
      },
    },
  },
  meilisearch: {
    fields: {
      index_name: {
        description: "Meilisearch index name to publish to",
        staticValue: "<empty>",
        staticFormField: "index_name",
        dynamicDefault: "sequin.<table_schema>.<table_name>",
      },
    },
  },
  elasticsearch: {
    fields: {
      index_name: {
        description: "Elasticsearch index name to publish to",
        staticValue: "<empty>",
        staticFormField: "index_name",
        dynamicDefault: "sequin.<database_name>.<table_schema>.<table_name>",
      },
    },
  },
  sqs: {
    fields: {
      queue_url: {
        description: "AWS SQS queue URL",
        staticValue: "<empty>",
        staticFormField: "queue_url",
        dynamicDefault: "<empty>",
      },
    },
  },
};
