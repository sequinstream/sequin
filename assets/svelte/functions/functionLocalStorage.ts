import type { FormData } from "./types";

const KEY_PREFIX = "function_code";
function getStorageKey(type: string, sinkType?: string) {
  if (type === "routing") {
    return `${KEY_PREFIX}_${type}_${sinkType}`;
  }
  return `${KEY_PREFIX}_${type}`;
}

export function saveFunctionCodeToStorage(form: FormData) {
  // Only save code for functions with the type assigned, and also sink_type assigned for routing functions
  if (
    form.function.type === undefined ||
    (form.function.type === "routing" && form.function.sink_type === undefined)
  ) {
    return;
  }
  const key = getStorageKey(form.function.type, form.function.sink_type);
  localStorage.setItem(key, form.function.code);
}

export function loadFunctionCodeFromStorage(
  type: string,
  sinkType?: string,
): string | null {
  const key = getStorageKey(type, sinkType);
  return localStorage.getItem(key);
}

export function clearAllFunctionCodeStorage() {
  Object.keys(localStorage)
    .filter((key) => key.startsWith(`${KEY_PREFIX}_`))
    .forEach((key) => localStorage.removeItem(key));
}
