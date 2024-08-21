<script lang="ts">
  import { createEventDispatcher } from "svelte";
  import NewTopnav from "./NewTopnav.svelte";

  export let formData: {
    name: string;
    database: string;
    hostname: string;
    port: number;
    username: string;
    password: string;
    ssl: boolean;
  };
  export let formErrors: Record<string, string | string[]>;
  export let validating: boolean;
  export let parent: string;
  export let live;

  const dispatch = createEventDispatcher();

  function handleSubmit(event: Event) {
    event.preventDefault();
    pushEvent("save", { postgres_database: formData });
  }

  // Update this reactive statement
  $: pushEvent("validate", { postgres_database: formData });

  // Update the getError function to hide errors for empty fields
  $: getError = (field: string): string | undefined => {
    if (!formData[field]) return undefined;
    const error = formErrors[field];
    if (Array.isArray(error)) {
      return error.join(", ");
    }
    return error;
  };

  // Add this function to push events to the parent LiveView
  function pushEvent(event: string, payload: any) {
    live.pushEventTo(`#${parent}`, event, payload);
  }
</script>

<div class="container mx-auto py-10">
  <div class="flex justify-between items-center mb-4">
    <h1 class="text-2xl font-bold">Create Database</h1>
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
      <label for="database" class="block text-sm font-medium text-gray-700"
        >Database</label
      >
      <input
        type="text"
        id="database"
        bind:value={formData.database}
        class="mt-1 block w-full shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
      />
      {#if getError("database")}<p class="mt-2 text-sm text-red-600">
          {getError("database")}
        </p>{/if}
    </div>

    <div>
      <label for="hostname" class="block text-sm font-medium text-gray-700"
        >Hostname</label
      >
      <input
        type="text"
        id="hostname"
        bind:value={formData.hostname}
        class="mt-1 block w-full shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
      />
      {#if getError("hostname")}<p class="mt-2 text-sm text-red-600">
          {getError("hostname")}
        </p>{/if}
    </div>

    <div>
      <label for="port" class="block text-sm font-medium text-gray-700"
        >Port</label
      >
      <input
        type="number"
        id="port"
        bind:value={formData.port}
        class="mt-1 block w-full shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
      />
      {#if getError("port")}<p class="mt-2 text-sm text-red-600">
          {getError("port")}
        </p>{/if}
    </div>

    <div>
      <label for="username" class="block text-sm font-medium text-gray-700"
        >Username</label
      >
      <input
        type="text"
        id="username"
        bind:value={formData.username}
        class="mt-1 block w-full shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
      />
      {#if getError("username")}<p class="mt-2 text-sm text-red-600">
          {getError("username")}
        </p>{/if}
    </div>

    <div>
      <label for="password" class="block text-sm font-medium text-gray-700"
        >Password</label
      >
      <input
        type="password"
        id="password"
        bind:value={formData.password}
        class="mt-1 block w-full shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
      />
      {#if getError("password")}<p class="mt-2 text-sm text-red-600">
          {getError("password")}
        </p>{/if}
    </div>

    <div>
      <label for="ssl" class="block text-sm font-medium text-gray-700"
        >SSL</label
      >
      <input
        type="checkbox"
        id="ssl"
        bind:checked={formData.ssl}
        class="mt-1 focus:ring-indigo-500 h-4 w-4 text-indigo-600 border-gray-300 rounded"
      />
      {#if getError("ssl")}<p class="mt-2 text-sm text-red-600">
          {getError("ssl")}
        </p>{/if}
    </div>

    <div>
      <button
        type="submit"
        disabled={validating}
        class="inline-flex justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500"
      >
        {validating ? "Validating..." : "Create Database"}
      </button>
    </div>
  </form>
</div>
