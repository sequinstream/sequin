import type { SinkType } from "./types";

type SinkRoutingDocs = {
  fields: {
    [key: string]: { description: string; default: string };
  };
};

export const sinkRoutingDocs: Record<SinkType, SinkRoutingDocs> = {
  http_push: {
    fields: {
      method: {
        description: "HTTP method (GET, POST, PUT, etc.)",
        default: "POST",
      },
      endpoint_path: {
        description: "Path portion of the URL",
        default: "/<endpoint-path>",
      },
    },
  },
  redis_string: {
    fields: {
      key: {
        description: "The Redis key to store the message in",
        default: "sequin:<table-name>:<primary-keys>",
      },
    },
  },
  nats: {
    fields: {
      subject: {
        description: "The NATS subject to publish messages to",
        default: "<table-name>.<action>.<primary-keys>",
      },
    },
  },
};
