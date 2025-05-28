// Base Function interface with shared properties
export interface Function {
  id: string;
  name: string;
  type: string;
  description: string;
  inserted_at: string;
  updated_at: string;
  function: PathFunction | FunctionFunction | RoutingFunction | FilterFunction;
}

// Path Transform specific configuration
export interface PathFunction {
  type: "path";
  path: string;
}

// Function Transform specific configuration
export interface FunctionFunction {
  type: "function";
  code: string;
}

// Routing Function specific configuration
export interface RoutingFunction {
  type: "routing";
  code: string;
  sink_type?: string;
}

// Filter Function specific configuration
export interface FilterFunction {
  type: "filter";
  code: string;
  description: string;
}

export interface FormData {
  id: string;
  name: string;
  description: string;
  function: {
    type: string;
    path?: string;
    code?: string;
    sink_type?: string;
  };
  modified_test_messages?: Record<string, TestMessage>;
}

export interface FormErrors {
  name?: string[];
  description?: string[];
  function?: {
    type?: string[];
    path?: string[];
    code?: string[];
    sink_type?: string[];
  };
  modified_test_messages?: Record<string, TestMessageError>;
}

export interface TestMessage {
  id: string;
  record: string;
  changes: string;
  action: string;
  metadata: string;
  transformed: string;
  time: number;
  error: {
    type: string;
    info: any;
  };
}

export interface Consumer {
  name: string;
}

export interface TestMessageError {
  record: string;
  changes: string;
  metadata: string;
}
