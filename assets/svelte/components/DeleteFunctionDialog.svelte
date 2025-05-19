<script lang="ts">
  import {
    AlertDialog,
    AlertDialogContent,
    AlertDialogDescription,
    AlertDialogFooter,
    AlertDialogHeader,
    AlertDialogTitle,
  } from "$lib/components/ui/alert-dialog";
  import { Button } from "$lib/components/ui/button";

  export let open: boolean;
  export let consumers: Array<{ name: string }>;
  export let onDelete: () => void = () => {};
</script>

<AlertDialog bind:open>
  <AlertDialogContent>
    <AlertDialogHeader>
      <AlertDialogTitle>
        {#if consumers.length > 0}
          Cannot delete function
        {:else}
          Confirm delete
        {/if}
      </AlertDialogTitle>

      <AlertDialogDescription>
        {#if consumers.length > 0}
          This function is currently used by:
          <ul class="list-disc pl-4 mt-2 space-y-1">
            {#each consumers as consumer}
              <li class="font-mono">{consumer.name}</li>
            {/each}
          </ul>
          <p class="mt-2">
            Please remove this function from all consumers before deleting it.
          </p>
        {:else}
          Are you sure you want to delete this function? This action cannot be
          undone.
        {/if}
      </AlertDialogDescription>
    </AlertDialogHeader>

    <AlertDialogFooter>
      <Button type="button" variant="default" on:click={() => (open = false)}>
        {consumers.length > 0 ? "OK" : "Cancel"}
      </Button>

      {#if consumers.length === 0}
        <Button
          type="button"
          variant="destructive"
          on:click={() => {
            onDelete();
            open = false;
          }}
        >
          Delete
        </Button>
      {/if}
    </AlertDialogFooter>
  </AlertDialogContent>
</AlertDialog>
