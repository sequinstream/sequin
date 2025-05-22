// Base Transform interface with shared properties
export interface Transform {
  id: string;
  name: string;
  type: string;
  description: string;
  inserted_at: string;
  updated_at: string;
  transform: PathTransform | FunctionTransform | RoutingTransform;
}

// Path Transform specific configuration
export interface PathTransform {
  type: "path";
  path: string;
}

// Function Transform specific configuration
export interface FunctionTransform {
  type: "function";
  code: string;
}

// Routing Transform specific configuration
export interface RoutingTransform {
  type: "routing";
  code: string;
  sink_type?: string;
}
