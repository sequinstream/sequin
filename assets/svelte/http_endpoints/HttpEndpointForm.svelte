<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import { PlusCircle } from "lucide-svelte";
  import { createEventDispatcher } from "svelte";

  export let formData: {
    name: string;
    base_url: string;
    headers: Record<string, string>;
  };
  export let formErrors: Record<string, string | string[]>;
  export let touchedFields: Set<string> = new Set();
  export let isSubmitted = false;

  const dispatch = createEventDispatcher();

  function notifyFormDataChange() {
    dispatch("formDataChange");
  }

  function updateFormData(field: string, value: string) {
    formData[field] = value;
    touchedFields.add(field);
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

  $: console.log("formErrors", formErrors);

  $: getError = (field: string): string | undefined => {
    if (!formErrors[field]) return undefined;
    if (formData[field] || touchedFields.has(field) || isSubmitted) {
      const error = formErrors[field];
      return Array.isArray(error) ? error.join(", ") : error;
    }
    return undefined;
  };
</script>

<div class="space-y-4">
  <div class="space-y-2">
    <Label for="name">Name</Label>
    <Input
      id="name"
      value={formData.name}
      on:input={(e) => updateFormData("name", e.currentTarget.value)}
      on:focus={() => touchedFields.add("name")}
      autocomplete="off"
      data-1p-ignore
    />
    {#if getError("name")}
      <p class="text-sm text-red-500">{getError("name")}</p>
    {/if}
  </div>

  <div class="space-y-2">
    <Label for="base_url">Base URL</Label>
    <Input
      id="base_url"
      value={formData.base_url}
      on:input={(e) => updateFormData("base_url", e.currentTarget.value)}
      on:focus={() => touchedFields.add("base_url")}
    />
    {#if getError("base_url")}
      <p class="text-sm text-red-500">{getError("base_url")}</p>
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
          class="text-carbon-400 hover:text-carbon-600 justify-self-end"
        >
          <icon class="hero-x-mark w-4 h-4" />
        </button>
      </div>
    {/each}
    <div class="grid grid-cols-1 gap-4 max-w-fit">
      <Button variant="outline" size="sm" on:click={addHeader} class="mt-2">
        <PlusCircle class="w-4 h-4 mr-2" />
        Add Header
      </Button>
    </div>
  </div>
</div>
