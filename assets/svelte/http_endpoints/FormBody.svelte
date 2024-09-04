<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import { PlusCircle, Eye, EyeOff } from "lucide-svelte";

  export let form: {
    name: string;
    baseUrl: string;
    headers: Record<string, string>;
    encryptedHeaders: Record<string, string>;
  };

  export let errors: Record<string, any> = {};

  let showEncryptedValues: Record<string, boolean> = {};

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

<div class="space-y-4">
  <div class="space-y-2">
    <Label for="name">Name</Label>
    <Input
      id="http-endpoint-name"
      bind:value={form.name}
      autocomplete="off"
      data-1p-ignore
      placeholder="Enter a unique name for your HTTP endpoint"
    />
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
    {#if errors.base_url}
      <p class="text-sm text-destructive">{errors.base_url}</p>
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
          on:input={(e) => updateHeaderValue(key, e.currentTarget.value)}
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
          on:input={(e) => updateEncryptedHeaderKey(key, e.currentTarget.value)}
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
