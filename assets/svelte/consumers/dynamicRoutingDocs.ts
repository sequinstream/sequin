import type { RoutedSinkType } from "./types";

type RoutedSinkDocs = {
  fields: {
    [key: string]: { description: string; default: string };
  };
};

export const routedSinkDocs: Record<RoutedSinkType, RoutedSinkDocs> = {
  http_push: {
    fields: {
      method: {
        description: "HTTP method (GET, POST, PUT, etc.)",
        default: "POST",
      },
      endpoint_path: {
        description: "Path portion of the URL, it is appended to the base URL",
        default: "",
      },
      headers: {
        description:
          "Map of key value pairs, the values configured in the HTTP endpoint are included",
        default: "%{}",
      },
    },
  },
  redis_string: {
    fields: {
      key: {
        description: "The Redis key to store the message in",
        default: "sequin:<table-name>:<primary-keys>",
      },
      action: {
        description: "The Redis action to perform",
        default: "'set' for insert, update, and read, 'del' for delete",
      },
      expire_ms: {
        description:
          "The expiration time in milliseconds, if nil the key will not expire",
        default: "nil",
      },
    },
  },
  nats: {
    fields: {
      subject: {
        description: "The NATS subject to publish messages to",
        default: "sequin.<database_name>.<table_schema>.<table_name>.<action>",
      },
      headers: {
        description: "Map of key value pairs",
        default: '%{"Idempotency-Key" => <idempotency-key>}',
      },
    },
  },
};
