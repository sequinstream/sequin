<script lang="ts">
  import { createEventDispatcher } from "svelte";
  import NewTopnav from "./NewTopnav.svelte";

  export let formData: {
    database: {
      name: string;
      database: string;
      hostname: string;
      port: number;
      username: string;
      password: string;
      ssl: boolean;
    };
    replication: {
      publication_name: string;
      slot_name: string;
    };
  };
  export let formErrors: Record<string, string | string[]>;
  export let validating: boolean;
  export let parent: string;
  export let live;

  const dispatch = createEventDispatcher();

  let userInput = false;

  function handleInput() {
    userInput = true;
  }

  function handleSubmit(event: Event) {
    event.preventDefault();
    pushEvent("save", {
      postgres_database: formData.database,
      postgres_replication_slot: formData.replication,
    });
  }

  $: if (userInput) {
    pushEvent("validate", {
      postgres_database: formData.database,
      postgres_replication_slot: formData.replication,
    });
    userInput = false;
  }

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
</script>

<div class="container mx-auto py-10">
  <div class="flex justify-between items-center mb-4">
    <h1 class="text-2xl font-bold">Create Database</h1>
  </div>

  <form on:submit={handleSubmit} class="space-y-4">
    <h2 class="text-xl font-semibold mb-4">Database Configuration</h2>
    <div>
      <label for="name" class="block text-sm font-medium text-gray-700"
        >Name</label
      >
      <input
        type="text"
        id="name"
        bind:value={formData.database.name}
        on:input={handleInput}
        class="mt-1 block w-full shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
      />
      {#if getError("database.name")}<p class="mt-2 text-sm text-red-600">
          {getError("database.name")}
        </p>{/if}
    </div>

    <div>
      <label for="database" class="block text-sm font-medium text-gray-700"
        >Database</label
      >
      <input
        type="text"
        id="database"
        bind:value={formData.database.database}
        on:input={handleInput}
        class="mt-1 block w-full shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
      />
      {#if getError("database.database")}<p class="mt-2 text-sm text-red-600">
          {getError("database.database")}
        </p>{/if}
    </div>

    <div>
      <label for="hostname" class="block text-sm font-medium text-gray-700"
        >Hostname</label
      >
      <input
        type="text"
        id="hostname"
        bind:value={formData.database.hostname}
        on:input={handleInput}
        class="mt-1 block w-full shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
      />
      {#if getError("database.hostname")}<p class="mt-2 text-sm text-red-600">
          {getError("database.hostname")}
        </p>{/if}
    </div>

    <div>
      <label for="port" class="block text-sm font-medium text-gray-700"
        >Port</label
      >
      <input
        type="number"
        id="port"
        bind:value={formData.database.port}
        on:input={handleInput}
        class="mt-1 block w-full shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
      />
      {#if getError("database.port")}<p class="mt-2 text-sm text-red-600">
          {getError("database.port")}
        </p>{/if}
    </div>

    <div>
      <label for="username" class="block text-sm font-medium text-gray-700"
        >Username</label
      >
      <input
        type="text"
        id="username"
        bind:value={formData.database.username}
        on:input={handleInput}
        class="mt-1 block w-full shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
      />
      {#if getError("database.username")}<p class="mt-2 text-sm text-red-600">
          {getError("database.username")}
        </p>{/if}
    </div>

    <div>
      <label for="password" class="block text-sm font-medium text-gray-700"
        >Password</label
      >
      <input
        type="password"
        id="password"
        bind:value={formData.database.password}
        on:input={handleInput}
        class="mt-1 block w-full shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
      />
      {#if getError("database.password")}<p class="mt-2 text-sm text-red-600">
          {getError("database.password")}
        </p>{/if}
    </div>

    <div>
      <label for="ssl" class="block text-sm font-medium text-gray-700"
        >SSL</label
      >
      <input
        type="checkbox"
        id="ssl"
        bind:checked={formData.database.ssl}
        on:change={handleInput}
        class="mt-1 focus:ring-indigo-500 h-4 w-4 text-indigo-600 border-gray-300 rounded"
      />
      {#if getError("database.ssl")}<p class="mt-2 text-sm text-red-600">
          {getError("database.ssl")}
        </p>{/if}
    </div>

    <div class="mt-8">
      <h2 class="text-xl font-semibold mb-4">Replication Configuration</h2>
      <p class="mb-4">
        To set up replication, you need to create a replication slot. Run the
        following SQL command on your database:
      </p>
      <pre
        class="bg-gray-100 p-4 rounded-md mb-4">SELECT pg_create_logical_replication_slot('{formData
          .replication.slot_name || "my_slot"}', 'pgoutput');</pre>
      <p class="mb-4">
        Next, you need to create a publication. You have two options:
      </p>
      <p class="mb-2">1. Create a publication for all tables:</p>
      <pre class="bg-gray-100 p-4 rounded-md mb-4">CREATE PUBLICATION {formData
          .replication.publication_name || "my_pub"} FOR ALL TABLES;</pre>
      <p class="mb-2">2. Create a publication for specific tables:</p>
      <pre class="bg-gray-100 p-4 rounded-md mb-4">CREATE PUBLICATION {formData
          .replication.publication_name ||
          "my_pub"} FOR TABLE table1, table2, table3;</pre>
    </div>

    <div>
      <label for="slot_name" class="block text-sm font-medium text-gray-700">
        Slot Name
      </label>
      <input
        type="text"
        id="slot_name"
        bind:value={formData.replication.slot_name}
        on:input={handleInput}
        class="mt-1 block w-full shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
      />
      {#if getError("replication.slot_name")}<p
          class="mt-2 text-sm text-red-600"
        >
          {getError("replication.slot_name")}
        </p>{/if}
    </div>
    <div>
      <label
        for="publication_name"
        class="block text-sm font-medium text-gray-700"
      >
        Publication Name
      </label>
      <input
        type="text"
        id="publication_name"
        bind:value={formData.replication.publication_name}
        on:input={handleInput}
        class="mt-1 block w-full shadow-sm sm:text-sm focus:ring-indigo-500 focus:border-indigo-500 border-gray-300 rounded-md"
      />
      {#if getError("replication.publication_name")}<p
          class="mt-2 text-sm text-red-600"
        >
          {getError("replication.publication_name")}
        </p>{/if}
    </div>

    <div>
      <button
        type="submit"
        disabled={validating}
        class="inline-flex justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500"
      >
        {validating ? "Validating..." : "Create Database and Replication Slot"}
      </button>
    </div>
  </form>
</div>
