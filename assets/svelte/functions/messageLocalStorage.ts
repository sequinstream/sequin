import type { FieldType, TestMessage } from "./types";

export function getStorageKey(message: TestMessage, field: FieldType) {
  return `editable_argument_${message.idempotency_key}_${field}`;
}

export function clearStorage(message: TestMessage, field: FieldType) {
  localStorage.removeItem(getStorageKey(message, field));
}

export function saveToStorage(message: TestMessage, field: FieldType) {
  localStorage.setItem(getStorageKey(message, field), message[field]);
}

export function loadFromStorage(message: TestMessage, field: FieldType) {
  return localStorage.getItem(getStorageKey(message, field));
}
