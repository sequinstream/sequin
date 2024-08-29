import { writable } from "svelte/store";

// Function to get the initial state from localStorage or default to false
const getInitialState = () => {
  const storedState = localStorage.getItem("isNavCollapsed");
  return storedState ? JSON.parse(storedState) : false;
};

// Create the writable store with the initial state
const isNavCollapsedStore = writable(getInitialState());

// Custom store to handle localStorage updates
export const isNavCollapsed = {
  ...isNavCollapsedStore,
  set: (value) => {
    isNavCollapsedStore.set(value);
    localStorage.setItem("isNavCollapsed", JSON.stringify(value));
  },
};
