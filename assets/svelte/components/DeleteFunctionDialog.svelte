<script lang="ts">
  import {
    AlertDialog,
    AlertDialogAction,
    AlertDialogContent,
    AlertDialogDescription,
    AlertDialogFooter,
    AlertDialogHeader,
    AlertDialogTitle,
  } from "$lib/components/ui/alert-dialog";

  export let open: boolean;
  export let consumers: Array<{ name: string }>;
  export let onDelete: () => void = () => {};
</script>

<AlertDialog bind:open>
  <AlertDialogContent>
    <AlertDialogHeader>
      <AlertDialogTitle>
        {#if consumers.length > 0}
          Cannot Delete Function
        {:else}
          Confirm Delete
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
      <AlertDialogAction on:click={() => consumers.length === 0 && onDelete()}>
        {consumers.length > 0 ? "OK" : "Delete"}
      </AlertDialogAction>
    </AlertDialogFooter>
  </AlertDialogContent>
</AlertDialog>
