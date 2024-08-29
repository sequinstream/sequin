<script>
  import CopyIcon from "$lib/components/CopyIcon.svelte";
  import * as Tooltip from "$lib/components/ui/tooltip";

  export let tabs;
  export let secret;

  let tabNames = tabs.map((tab) => tab.name);
  let selectedTab = tabs[0];
  let showSecret = false;
  let codeForDisplay;
  let codeForCopy;

  $: {
    codeForCopy = selectedTab.value.replace(/{{secret}}/g, secret);
  }

  $: {
    codeForDisplay = selectedTab.value.replace(
      /{{secret}}/g,
      showSecret ? secret : "********"
    );
  }
</script>

<div class="bg-gray-50 rounded-xl border border-border p-2 relative">
  <div class="flex justify-between items-center pl-2">
    <div class="flex space-x-4">
      <div class="relative">
        {#each tabNames as tabName}
          <button
            on:click={() =>
              (selectedTab = tabs.find((tab) => tab.name === tabName))}
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
      class="flex items-center space-x-2 text-muted-foreground px-2 py-1 rounded"
    >
      <div class="cursor-pointer">
        <button class="icon-button" on:click={() => (showSecret = !showSecret)}>
          <icon class:hidden={showSecret} class="hero-eye text-gray-500" />
          <icon
            class:hidden={!showSecret}
            class="hero-eye-slash text-gray-500"
          />
        </button>
      </div>
      <div class="cursor-pointer">
        <CopyIcon content={codeForCopy} />
      </div>
    </div>
  </div>
  <div class="border-b-2 border-border mb-2"></div>
  <pre class="text-sm bg-gray-50 rounded-xl p-4 overflow-x-auto leading-6"><code
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
