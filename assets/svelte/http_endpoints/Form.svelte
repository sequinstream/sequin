<script lang="ts">
  import FullPageModal from "../components/FullPageModal.svelte";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import FormBody from "./FormBody.svelte";

  export let httpEndpoint: {
    id?: string;
    name: string;
    base_url: string;
    headers: Record<string, string>;
  };
  export let errors: Record<string, any> = {};
  export let parent: string;
  export let live;

  let isEdit = !!httpEndpoint.id;
  let userInput = false;
  let dialogOpen = true;
  let showConfirmDialog = false;
  let validating = false;

  function pushEvent(event: string, payload = {}, callback = () => {}) {
    live.pushEventTo(`#${parent}`, event, payload, callback);
  }

  function handleInput() {
    userInput = true;
  }

  function handleSubmit(event: Event) {
    event.preventDefault();
    validating = true;
    pushEvent(
      "http_endpoint_submitted",
      { http_endpoint: httpEndpoint },
      () => {
        validating = false;
      }
    );
  }

  $: if (userInput) {
    pushEvent("http_endpoint_updated", { http_endpoint: httpEndpoint });
    userInput = false;
  }

  function handleClose() {
    pushEvent("form_closed");
  }
</script>

<FullPageModal
  title={isEdit ? "Edit HTTP Endpoint" : "Create HTTP Endpoint"}
  bind:open={dialogOpen}
  bind:showConfirmDialog
  on:close={handleClose}
>
  <form on:submit={handleSubmit} class="space-y-6 max-w-3xl mx-auto">
    <Card>
      <CardHeader>
        <CardTitle>HTTP Endpoint Configuration</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <FormBody {httpEndpoint} {errors} on:input={handleInput} />
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>HTTP Endpoint</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <Button type="submit" disabled={validating} variant="default">
          {#if validating}
            <span class="loading loading-spinner" />
            Validating...
          {:else if isEdit}
            Update HTTP Endpoint
          {:else}
            Create HTTP Endpoint
          {/if}
        </Button>
      </CardContent>
    </Card>
  </form>
</FullPageModal>
