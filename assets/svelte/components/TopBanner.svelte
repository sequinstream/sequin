<script lang="ts">
  import { Alert, AlertDescription } from "$lib/components/ui/alert";
  import { AlertTriangle } from "lucide-svelte";
  import * as AlertDialog from "$lib/components/ui/alert-dialog";
  import { Button } from "$lib/components/ui/button";
  import { formatRelativeTimestamp } from "$lib/utils";

  export let expiresAt: string;
  export let almostExpired: boolean;

  let showExplainerDialog = false;
</script>

<div class="w-full bg-yellow-100 border-b border-yellow-200">
  <div class="mx-auto">
    <Alert variant="default" class="bg-transparent border-none shadow-none p-3">
      <div class="flex items-center gap-4">
        <AlertTriangle class="h-4 w-4 text-yellow-600" />
        <AlertDescription class="text-yellow-800 text-sm">
          {#if almostExpired}
            This is a temporary account. Your data is pending removal soon.
          {:else}
            This is a temporary account. Your data will be deleted {formatRelativeTimestamp(
              expiresAt
            )}.
            <button
              class="underline hover:text-yellow-900"
              on:click={() => (showExplainerDialog = true)}
            >
              Learn more
            </button>
          {/if}
        </AlertDescription>
      </div>
    </Alert>
  </div>
</div>

<AlertDialog.Root bind:open={showExplainerDialog}>
  <AlertDialog.Content>
    <AlertDialog.Header>
      <AlertDialog.Title>About temporary accounts</AlertDialog.Title>
      <AlertDialog.Description>
        <p class="mb-4">
          Sequin's hosted solution is still in preview. At this time, we don't
          have permanent accounts. Permanent accounts are coming soon.
        </p>
        <p>
          In the meantime, be aware that your data is not stored permanently. It
          is also not accessible if you log out.
        </p>
      </AlertDialog.Description>
    </AlertDialog.Header>
    <AlertDialog.Footer>
      <AlertDialog.Action on:click={() => (showExplainerDialog = false)}
        >Close</AlertDialog.Action
      >
    </AlertDialog.Footer>
  </AlertDialog.Content>
</AlertDialog.Root>
