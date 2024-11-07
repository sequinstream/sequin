<script>
  import CopyIcon from "$lib/components/CopyIcon.svelte";
  import {
    Select,
    SelectTrigger,
    SelectValue,
    SelectContent,
    SelectItem,
    SelectLabel,
  } from "$lib/components/ui/select";
  import { Label } from "$lib/components/ui/label";
  import { Button } from "$lib/components/ui/button";
  export let tabs;
  export let apiTokens = [];
  export let containerClass = "";
  export let preClass = "";

  let tabNames = tabs.map((tab) => tab.name);
  let selectedTabIdx = 0;
  let showApiToken = false;
  let selectedApiToken = apiTokens.length > 0 ? apiTokens[0] : null;
  let codeForDisplay;
  let codeForCopy;

  $: selectedTab = tabs[selectedTabIdx];

  $: {
    codeForCopy = selectedTab.value.replace(
      /{{secret}}/g,
      selectedApiToken?.token || "{{CREATE_AN_API_TOKEN}}",
    );
  }

  $: {
    codeForDisplay = selectedTab.value.replace(
      /{{secret}}/g,
      showApiToken
        ? selectedApiToken?.token || "{{CREATE_AN_API_TOKEN}}"
        : "********",
    );
  }
</script>

<div
  class="bg-muted rounded-xl border border-border p-2 relative {containerClass}"
>
  <div class="flex justify-between items-center pl-2">
    <div class="flex space-x-4">
      <div class="relative">
        {#each tabNames as tabName}
          <button
            on:click|preventDefault={() =>
              (selectedTabIdx = tabs.findIndex((tab) => tab.name === tabName))}
            class="text-primary py-1">{tabName}</button
          >
          <div
            class="border-b-2 absolute inset-x-0 bottom-0"
            style="transform: translateY(6px)"
            class:border-primary={tabName === selectedTab.name}
            class:border-transparent={tabName !== selectedTab.name}
          ></div>
        {/each}
      </div>
    </div>
    <div
      class="flex items-center space-x-4 text-muted-foreground px-2 py-1 rounded"
    >
      {#if apiTokens.length > 0}
        <div class="flex flex-row items-center space-x-2">
          <Label class="text-sm text-gray-500">API Token</Label>
          <Select
            selected={{
              value: selectedApiToken.token,
              label: selectedApiToken.name,
            }}
            onSelectedChange={(event) => (selectedApiToken = event.value)}
          >
            <SelectTrigger class="w-[180px] bg-muted">
              <SelectValue placeholder="Select API Token" />
            </SelectTrigger>
            <SelectContent class="bg-muted text-gray-500">
              {#each apiTokens as apiToken}
                <SelectItem value={apiToken}>
                  {apiToken.name}
                </SelectItem>
              {/each}
            </SelectContent>
          </Select>
        </div>

        <div class="cursor-pointer">
          <button
            class="icon-button"
            on:click|preventDefault={() => (showApiToken = !showApiToken)}
          >
            <icon class:hidden={showApiToken} class="hero-eye text-gray-500" />
            <icon
              class:hidden={!showApiToken}
              class="hero-eye-slash text-gray-500"
            />
          </button>
        </div>
        <div class="cursor-pointer">
          <CopyIcon content={codeForCopy} />
        </div>
      {:else}
        <Button href="/settings/accounts" variant="link" size="sm">
          Add an API token
        </Button>
      {/if}
    </div>
  </div>
  <div class="border-b-2 border-border mb-2"></div>
  <pre
    class="text-sm bg-muted rounded-xl p-4 overflow-x-auto leading-6 {preClass}"><code
      class="text-foreground">{codeForDisplay}</code
    ></pre>
</div>

<style>
  code::before {
    content: "";
  }
  code::after {
    content: "";
  }
  .icon-button {
    background: none;
    border: none;
    padding: 0;
    cursor: pointer;
  }
</style>
