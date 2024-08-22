<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";

  export let formData: {
    name: string;
    base_url: string;
    headers: Record<string, string>;
  };
  export let formErrors: Record<string, string | string[]>;

  $: console.log("formErrors", formErrors);

  $: getError = (field: string): string | undefined => {
    console.log("field", field);
    console.log("formData", formData);
    if (!formErrors[field]) return undefined;
    const error = formErrors[field];
    if (Array.isArray(error)) {
      return error.join(", ");
    }
    return error;
  };

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

<div class="space-y-4">
  <div class="space-y-2">
    <Label for="name">Name</Label>
    <Input id="name" bind:value={formData.name} />
    {#if getError("name")}
      <p class="text-sm text-red-500">{getError("name")}</p>
    {/if}
  </div>

  <div class="space-y-2">
    <Label for="base_url">Base URL</Label>
    <Input id="base_url" bind:value={formData.base_url} />
    {#if getError("base_url")}
      <p class="text-sm text-red-500">{getError("base_url")}</p>
    {/if}
  </div>

  <div class="space-y-2">
    <Label>Headers</Label>
    <div class="space-y-2">
      {#each Object.entries(formData.headers) as [key, value]}
        <div class="flex items-center space-x-2">
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
          <Button
            variant="destructive"
            size="sm"
            on:click={() => removeHeader(key)}
          >
            Remove
          </Button>
        </div>
      {/each}
    </div>
    <Button variant="outline" on:click={addHeader}>Add Header</Button>
  </div>
</div>
