<script lang="ts">
  import { XCircle, RotateCw } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import * as Alert from "$lib/components/ui/alert";
  import type { Check } from "./Types";
  import { marked } from "marked";
  import CodeWithCopy from "$lib/components/CodeWithCopy.svelte";

  export let checks: Check[];
  export let pushEvent: (
    event: string,
    data: any,
    cb?: (val: any) => void,
  ) => void;

  let loadingStates: Record<string, boolean> = {};

  function handleRefreshCheck(slug: string) {
    loadingStates[slug] = true;
    pushEvent("refresh_check", { slug }, () => {
      loadingStates[slug] = false;
    });
  }

  function handleDismissCheck(slug: string, error_slug?: string) {
    pushEvent("dismiss_check", { slug, error_slug });
  }

  // Map status to alert variant
  function getAlertVariant(status: Check["status"]) {
    switch (status) {
      case "error":
        return "destructive";
      case "warning":
        return "warning";
      case "notice":
        return "info";
      default:
        return "default";
    }
  }
</script>

{#each checks as check}
  {#if check.alertMessage || check.error}
    <Alert.Root variant={getAlertVariant(check.status)} class="mb-6">
      <Alert.Title class="flex items-center justify-between">
        <span>{check.alertTitle || `Error: ${check.name}`}</span>
        <div class="space-x-2">
          {#if check.refreshable}
            <Button
              variant="outline"
              loading={loadingStates[check.slug]}
              size="sm"
              on:click={() => handleRefreshCheck(check.slug)}
            >
              <RotateCw class="h-4 w-4 mr-1" />
              Refresh
              <span slot="loading">Refreshing...</span>
            </Button>
          {/if}
          {#if check.dismissable}
            <Button
              variant="outline"
              size="sm"
              on:click={() => handleDismissCheck(check.slug, check.error_slug)}
            >
              <XCircle class="h-4 w-4 mr-1" />
              Dismiss
            </Button>
          {/if}
        </div>
      </Alert.Title>
      <Alert.Description class="mt-4">
        {#if check.alertMessage}
          <div class="prose prose-sm max-w-none">
            {@html marked(check.alertMessage)}
          </div>
        {/if}
        {#if check.code}
          <CodeWithCopy
            codeClass="mt-4 text-gray-600"
            language={check.code.language}
            code={check.code.code}
          />
        {/if}
        {#if check.error}
          <p class="text-sm py-2 rounded mt-2">
            {check.error.message}
          </p>
        {/if}
        {#if check.status === "stale"}
          <p class="text-sm text-yellow-600 bg-yellow-50 p-2 rounded mt-2">
            No update received from this check in the expected time interval.
            This is usually a bug - contact the Sequin team.
          </p>
        {/if}
      </Alert.Description>
    </Alert.Root>
  {/if}
{/each}
