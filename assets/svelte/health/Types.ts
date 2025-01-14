export type status =
  | "healthy"
  | "error"
  | "warning"
  | "initializing"
  | "stale"
  | "paused"
  | "notice";

export type Check = {
  name: string;
  slug: string;
  status: status;
  error?: {
    message: string;
  };
  error_slug?: string;
  alertTitle?: string;
  alertMessage?: string;
  refreshable?: boolean;
  dismissable?: boolean;
};

export type Health = {
  entity_kind: string;
  entity_id: string;
  name: string;
  status: status;
  checks: Check[];
};
