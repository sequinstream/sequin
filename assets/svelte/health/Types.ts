export type status = "healthy" | "error" | "warning" | "initializing" | "stale";

export type Check = {
  name: string;
  status: status;
  error?: {
    message: string;
  };
  message?: string;
};

export type Health = {
  entity_kind: string;
  entity_id: string;
  name: string;
  status: status;
  checks: Check[];
};
