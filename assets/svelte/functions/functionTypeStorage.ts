const FUNCTION_TYPE_KEY = "function_type";
const SINK_TYPE_KEY = "function_sink_type";

export function saveFunctionTypeToStorage(type: string) {
  localStorage.setItem(FUNCTION_TYPE_KEY, type);
}

export function saveSinkTypeToStorage(sinkType: string) {
  localStorage.setItem(SINK_TYPE_KEY, sinkType);
}

export function loadFunctionTypeFromStorage(): string | null {
  return localStorage.getItem(FUNCTION_TYPE_KEY);
}

export function loadSinkTypeFromStorage(): string | null {
  return localStorage.getItem(SINK_TYPE_KEY);
}

export function clearFunctionTypeStorage() {
  localStorage.removeItem(FUNCTION_TYPE_KEY);
  localStorage.removeItem(SINK_TYPE_KEY);
}
