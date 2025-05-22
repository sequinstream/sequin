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
