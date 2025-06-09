import type { FormData } from "./types";

const KEY_PREFIX = "function_code";
function getStorageKey(type: string, sinkType?: string, id?: string) {
  const keyFragment = type === "routing" ? `${type}_${sinkType}` : `${type}`;
  return `${KEY_PREFIX}_${id}__${keyFragment}`;
}

export function saveFunctionCodeToStorage(form: FormData) {
  // Only save code for functions with the type assigned, and also sink_type assigned for routing functions
  if (
    form.function.type === undefined ||
    (form.function.type === "routing" && form.function.sink_type === undefined)
  ) {
    return;
  }
  const key = getStorageKey(
    form.function.type,
    form.function.sink_type,
    form.id,
  );
  localStorage.setItem(key, form.function.code);
}

export function loadFunctionCodeFromStorage(
  type: string,
  sinkType?: string,
  id?: string,
): string | null {
  const key = getStorageKey(type, sinkType, id);
  return localStorage.getItem(key);
}

export function clearFunctionCodeStorage(id?: string) {
  Object.keys(localStorage)
    .filter((key) => key.startsWith(`${KEY_PREFIX}_${id}`))
    .forEach((key) => localStorage.removeItem(key));
}
