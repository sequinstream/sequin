<script lang="ts">
  import HttpEndpointForm from "./HttpEndpointForm.svelte";

  export let formData: {
    name: string;
    base_url: string;
    headers: Record<string, string>;
  };
  export let formErrors: Record<string, string | string[]>;
  export let parent: string;
  export let live;

  let userInput = false;

  function handleSubmit(event: Event) {
    event.preventDefault();
    pushEvent("save", { http_endpoint: formData });
  }

  $: {
    pushEvent("validate", { http_endpoint: formData });
  }

  function pushEvent(event: string, payload: any) {
    live.pushEventTo(`#${parent}`, event, payload);
  }
</script>

<div class="container mx-auto py-10">
  <div class="flex justify-between items-center mb-4">
    <h1 class="text-2xl font-bold">Create HTTP Endpoint</h1>
  </div>

  <form on:submit={handleSubmit} class="space-y-4">
    <HttpEndpointForm {formData} {formErrors} />

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
