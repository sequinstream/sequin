<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import { PlusCircle } from "lucide-svelte";
  import FullPageModal from "../components/FullPageModal.svelte";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";

  export let formData: {
    name: string;
    base_url: string;
    headers: Record<string, string>;
  };
  export let formErrors: Record<string, string | string[]>;
  export let parent: string;
  export let live;

  let isSubmitted = false;
  let userInput = false;
  let dialogOpen = true;
  let showConfirmDialog = false;

  function pushEvent(event: string, payload = {}) {
    live.pushEventTo(`#${parent}`, event, payload);
  }

  function handleSubmit(event: Event) {
    event.preventDefault();
    isSubmitted = true;
    pushEvent("save", { http_endpoint: formData });
  }

  function notifyFormDataChange() {
    userInput = true;
    pushEvent("validate", { http_endpoint: formData });
  }

  function updateFormData(field: string, value: string) {
    formData[field] = value;
    notifyFormDataChange();
  }

  function addHeader() {
    formData.headers = { ...formData.headers, "": "" };
    notifyFormDataChange();
  }

  function updateHeaderKey(oldKey: string, newKey: string) {
    const { [oldKey]: value, ...rest } = formData.headers;
    formData.headers = { ...rest, [newKey]: value };
    notifyFormDataChange();
  }

  function updateHeaderValue(key: string, value: string) {
    formData.headers = { ...formData.headers, [key]: value };
    notifyFormDataChange();
  }

  function removeHeader(key: string) {
    const { [key]: _, ...rest } = formData.headers;
    formData.headers = rest;
    notifyFormDataChange();
  }

  function handleClose() {
    pushEvent("form_closed");
  }

  $: getError = (field: string): string | undefined => {
    if (!formErrors[field]) return undefined;
    const error = formErrors[field];
    return Array.isArray(error) ? error.join(", ") : error;
  };

  $: {
    if (userInput) {
      pushEvent("validate", { http_endpoint: formData });
      userInput = false;
    }
  }
</script>

<FullPageModal
  title="Create HTTP Endpoint"
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
        <div class="space-y-2">
          <Label for="base_url">Base URL</Label>
          <Input
            id="base_url"
            value={formData.base_url}
            on:input={(e) => updateFormData("base_url", e.currentTarget.value)}
          />
          {#if getError("base_url")}
            <p class="text-sm text-destructive">{getError("base_url")}</p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label>Headers</Label>
          {#each Object.entries(formData.headers) as [key, value], index}
            <div class="grid grid-cols-[1fr_1fr_15px] gap-4 mb-2">
              <Input
                value={key}
                on:input={(e) => updateHeaderKey(key, e.currentTarget.value)}
                placeholder="Key"
              />
              <Input
                {value}
                on:input={(e) => updateHeaderValue(key, e.currentTarget.value)}
                placeholder="Value"
              />
              <button
                on:click={() => removeHeader(key)}
                class="text-muted-foreground hover:text-foreground justify-self-end"
              >
                <icon class="hero-x-mark w-4 h-4" />
              </button>
            </div>
          {/each}
          <div class="grid grid-cols-1 gap-4 max-w-fit">
            <Button
              variant="outline"
              size="sm"
              on:click={addHeader}
              class="mt-2"
            >
              <PlusCircle class="w-4 h-4 mr-2" />
              Add Header
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>HTTP Endpoint</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <Label for="name">Name</Label>
          <Input
            id="name"
            value={formData.name}
            on:input={(e) => updateFormData("name", e.currentTarget.value)}
            autocomplete="off"
            data-1p-ignore
            placeholder="Enter a unique name for your HTTP endpoint"
          />
          {#if getError("name")}
            <p class="text-sm text-destructive">{getError("name")}</p>
          {/if}
        </div>

        <Button type="submit">Create HTTP Endpoint</Button>
      </CardContent>
    </Card>
  </form>
</FullPageModal>
