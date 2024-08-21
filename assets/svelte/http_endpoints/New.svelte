<script lang="ts">
  import { createEventDispatcher } from "svelte";

  export let formData: {
    name: string;
    base_url: string;
    headers: Record<string, string>;
  };
  export let formErrors: Record<string, string | string[]>;
  export let validating: boolean;
  export let parent: string;
  export let live;

  const dispatch = createEventDispatcher();

  function handleSubmit(event: Event) {
    event.preventDefault();
    pushEvent("save", { http_endpoint: formData });
  }

  $: pushEvent("validate", { http_endpoint: formData });

  $: getError = (field: string): string | undefined => {
    if (!formData[field]) return undefined;
    const error = formErrors[field];
    if (Array.isArray(error)) {
      return error.join(", ");
    }
    return error;
  };

  function pushEvent(event: string, payload: any) {
    live.pushEventTo(`#${parent}`, event, payload);
  }

  function addHeader() {
    formData.headers = { ...formData.headers, "": "" };
  }

  function updateHeaderKey(oldKey: string, newKey: string) {
    const { [oldKey]: value, ...rest } = formData.headers;
    formData.headers = { ...rest, [newKey]: value };
  }

  function updateHeaderValue(key: string, value: string) {
    formData.headers = { ...formData.headers, [key]: value };
  }

  function removeHeader(key: string) {
    const { [key]: _, ...rest } = formData.headers;
    formData.headers = rest;
  }
</script>

<div class="container mx-auto py-10">
  <div class="flex justify-between items-center mb-4">
    <h1 class="text-2xl font-bold">Create HTTP Endpoint</h1>
  </div>

  <form on:submit={handleSubmit} class="space-y-4">
    <div>
      <label for="name" class="block text-sm font-medium text-gray-700"
        >Name</label
      >
      <input
        type="text"
        id="name"
        bind:value={formData.name}
        class="mt-1 block w-full shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
      />
      {#if getError("name")}<p class="mt-2 text-sm text-red-600">
          {getError("name")}
        </p>{/if}
    </div>

    <div>
      <label for="base_url" class="block text-sm font-medium text-gray-700"
        >Base URL</label
      >
      <input
        type="text"
        id="base_url"
        bind:value={formData.base_url}
        class="mt-1 block w-full shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
      />
      {#if getError("base_url")}<p class="mt-2 text-sm text-red-600">
          {getError("base_url")}
        </p>{/if}
    </div>

    <div>
      <label class="block text-sm font-medium text-gray-700">Headers</label>
      {#each Object.entries(formData.headers) as [key, value]}
        <div class="flex space-x-2 mt-2">
          <input
            type="text"
            value={key}
            on:input={(e) => updateHeaderKey(key, e.target.value)}
            placeholder="Key"
            class="flex-1 shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
          />
          <input
            type="text"
            {value}
            on:input={(e) => updateHeaderValue(key, e.target.value)}
            placeholder="Value"
            class="flex-1 shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
          />
          <button
            type="button"
            on:click={() => removeHeader(key)}
            class="text-red-600">Remove</button
          >
        </div>
      {/each}
      <button type="button" on:click={addHeader} class="mt-2 text-indigo-600"
        >Add Header</button
      >
    </div>

    <div>
      <button
        type="submit"
        class="inline-flex justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500"
      >
        Create HTTP Endpoint
      </button>
    </div>
  </form>
</div>
