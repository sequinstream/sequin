import type { SinkType } from "./types";

type SinkRoutingDocs = {
  fields: { [key: string]: string };
  defaultInfo: {
    pattern: string;
    entityKind: string;
  };
};

export const sinkRoutingDocs: Record<SinkType, SinkRoutingDocs> = {
  http_push: {
    fields: {
      method: "HTTP method (GET, POST, PUT, etc.)",
      endpoint_path: "Path portion of the URL",
    },
    defaultInfo: {
      pattern: "POST /<endpoint-path>",
      entityKind: "request",
    },
  },
  redis_string: {
    fields: {
      key: "The Redis key to store the message in",
    },
    defaultInfo: {
      pattern: "sequin:<table-name>:<primary-keys>",
      entityKind: "key",
    },
  },
  nats: {
    fields: {
      subject: "The NATS subject to publish messages to",
    },
    defaultInfo: {
      pattern: "<table-name>.<action>.<primary-keys>",
      entityKind: "subject",
    },
  },
};
