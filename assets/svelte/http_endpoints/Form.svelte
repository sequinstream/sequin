<script lang="ts">
  import FullPageModal from "../components/FullPageModal.svelte";
  import { Button } from "$lib/components/ui/button";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import { PlusCircle, Eye, EyeOff } from "lucide-svelte";

  export let httpEndpoint: {
    id?: string;
    name: string;
    baseUrl: string;
    headers: Record<string, string>;
    encryptedHeaders: Record<string, string>;
  };
  export let errors: Record<string, any> = {};
  export let parent: string;
  export let live;

  let form = { ...httpEndpoint };
  let isEdit = !!form.id;
  let dialogOpen = true;
  let showConfirmDialog = false;
  let validating = false;
  let showEncryptedValues: Record<string, boolean> = {};

  $: baseUrlError =
    errors.host ||
    errors.scheme ||
    errors.port ||
    errors.path ||
    errors.query ||
    errors.fragment
      ? "The URL is invalid. Please include a scheme (e.g. http:// or https://) and a full hostname."
      : "";

  function pushEvent(
    event: string,
    payload = {},
    callback = (event: any) => {}
  ) {
    live.pushEventTo(`#${parent}`, event, payload, callback);
  }

  function handleSubmit(event: Event) {
    event.preventDefault();
    pushEvent("form_submitted", { form }, (reply) => {
      if (reply?.ok !== true) {
        validating = false;
      }
    });
  }

  $: pushEvent("form_updated", { form });

  function handleClose() {
    pushEvent("form_closed");
  }

  function addHeader() {
    form.headers = { ...form.headers, "": "" };
  }

  function updateHeaderKey(oldKey: string, newKey: string) {
    const { [oldKey]: value, ...rest } = form.headers;
    form.headers = { ...rest, [newKey]: value };
  }

  function updateHeaderValue(key: string, value: string) {
    form.headers = { ...form.headers, [key]: value };
  }

  function removeHeader(key: string) {
    const { [key]: _, ...rest } = form.headers;
    form.headers = rest;
  }

  function addEncryptedHeader() {
    form.encryptedHeaders = { ...form.encryptedHeaders, "": "" };
  }

  function updateEncryptedHeaderKey(oldKey: string, newKey: string) {
    const { [oldKey]: value, ...rest } = form.encryptedHeaders;
    form.encryptedHeaders = { ...rest, [newKey]: value };
  }

  function updateEncryptedHeaderValue(key: string, value: string) {
    form.encryptedHeaders = { ...form.encryptedHeaders, [key]: value };
  }

  function removeEncryptedHeader(key: string) {
    const { [key]: _, ...rest } = form.encryptedHeaders;
    form.encryptedHeaders = rest;
  }

  function toggleEncryptedValue(key: string) {
    showEncryptedValues[key] = !showEncryptedValues[key];
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
        <div class="space-y-4">
          <div class="space-y-2">
            <Label for="name">Name</Label>
            <div class="flex flex-col gap-1">
              <Input
                id="http-endpoint-name"
                bind:value={form.name}
                autocomplete="off"
                data-1p-ignore
                placeholder="Enter a unique name for your HTTP endpoint"
              />
              <p class="text-xs font-light">
                Must contain only alphanumeric characters, underscores, hyphens,
                or dots.
              </p>
            </div>
            {#if errors.name}
              <p class="text-sm text-destructive">{errors.name}</p>
            {/if}
          </div>

          <div class="space-y-2">
            <Label for="baseUrl">Base URL</Label>
            <Input
              id="http-endpoint-baseUrl"
              bind:value={form.baseUrl}
              placeholder="https://api.example.com"
            />
            {#if baseUrlError}
              <p class="text-sm text-destructive">{baseUrlError}</p>
            {/if}
          </div>

          <div class="space-y-2">
            <Label>Headers</Label>
            {#each Object.entries(form.headers) as [key, value], index}
              <div class="grid grid-cols-[1fr_1fr_15px] gap-4 mb-2">
                <Input
                  id="http-endpoint-header-key-{index}"
                  value={key}
                  on:input={(e) => updateHeaderKey(key, e.currentTarget.value)}
                  placeholder="Key"
                />
                <Input
                  id="http-endpoint-header-value-{index}"
                  {value}
                  on:input={(e) =>
                    updateHeaderValue(key, e.currentTarget.value)}
                  placeholder="Value"
                />
                <button
                  type="button"
                  on:click={() => removeHeader(key)}
                  class="text-muted-foreground hover:text-foreground justify-self-end"
                >
                  <icon class="hero-x-mark w-4 h-4" />
                </button>
              </div>
            {/each}
            <div class="grid grid-cols-1 gap-4 max-w-fit">
              <Button
                type="button"
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

          <div class="space-y-2">
            <Label>Encrypted Headers</Label>
            {#each Object.entries(form.encryptedHeaders) as [key, value], index}
              <div class="grid grid-cols-[1fr_1fr_auto_15px] gap-4 mb-2">
                <Input
                  id="http-endpoint-encrypted-header-key-{index}"
                  value={key}
                  on:input={(e) =>
                    updateEncryptedHeaderKey(key, e.currentTarget.value)}
                  placeholder="Key"
                />
                <Input
                  id="http-endpoint-encrypted-header-value-{index}"
                  {value}
                  on:input={(e) =>
                    updateEncryptedHeaderValue(key, e.currentTarget.value)}
                  placeholder="Value"
                  type={showEncryptedValues[key] ? "text" : "password"}
                />
                <button
                  type="button"
                  on:click={() => toggleEncryptedValue(key)}
                  class="text-muted-foreground hover:text-foreground"
                >
                  {#if showEncryptedValues[key]}
                    <EyeOff class="w-4 h-4" />
                  {:else}
                    <Eye class="w-4 h-4" />
                  {/if}
                </button>
                <button
                  type="button"
                  on:click={() => removeEncryptedHeader(key)}
                  class="text-muted-foreground hover:text-foreground justify-self-end"
                >
                  <icon class="hero-x-mark w-4 h-4" />
                </button>
              </div>
            {/each}
            <div class="grid grid-cols-1 gap-4 max-w-fit">
              <Button
                type="button"
                variant="outline"
                size="sm"
                on:click={addEncryptedHeader}
                class="mt-2"
              >
                <PlusCircle class="w-4 h-4 mr-2" />
                Add Encrypted Header
              </Button>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>HTTP Endpoint</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <Button type="submit" loading={validating} variant="default">
          <span slot="loading"> Validating... </span>
          {#if isEdit}
            Update HTTP Endpoint
          {:else}
            Create HTTP Endpoint
          {/if}
        </Button>
      </CardContent>
    </Card>
  </form>
</FullPageModal>
