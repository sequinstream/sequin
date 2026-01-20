<script lang="ts">
  import { Label } from "$lib/components/ui/label";
  import { Switch } from "$lib/components/ui/switch";
  import FunctionPicker from "$lib/consumers/FunctionPicker.svelte";

  export let form: {
    actions: string[];
    filterId: string;
  };
  export let showTitle: boolean = true;
  export let functions: Array<{
    id: string;
    name: string;
    type: string;
    description: string;
  }> = [];
  export let refreshFunctions: () => void = () => {};
  export let functionRefreshState: "idle" | "refreshing" | "done" = "idle";

  $: actions = form.actions || [];
  const switches = [
    { id: "insert", label: "Insert" },
    { id: "update", label: "Update" },
    { id: "delete", label: "Delete" },
  ];

  function handleFilterFunctionChange(functionId: string) {
    form.filterId = functionId;
  }
</script>

<div class="flex flex-col gap-8">
  {#if showTitle}
    <Label class="text-base font-medium">Filters</Label>
  {/if}

  <div class="flex flex-col gap-4">
    <div class="text-base font-medium">Operations to capture</div>
    <div class="flex items-center gap-4">
      {#each switches as { id, label }}
        <div class="flex items-center gap-2">
          <Label for={id} class="cursor-pointer">{label}</Label>
          <Switch
            {id}
            disabled={!form.postgresDatabaseId && !form.tableOid}
            checked={actions.includes(id)}
            onCheckedChange={(checked) => {
              const newActions = checked
                ? [...actions, id]
                : actions.filter((a) => a !== id);
              form.actions = newActions;
            }}
          />
        </div>
      {/each}
    </div>
  </div>

  <!-- Filter Function -->
  <div class="flex flex-col gap-4">
    <FunctionPicker
      {functions}
      selectedFunctionId={form.filterId || "none"}
      title="Filter function"
      onFunctionChange={handleFilterFunctionChange}
      {refreshFunctions}
      allowedFunctionTypes={["filter"]}
      newFunctionType="filter"
      createNewQueryParams="?type=filter"
      bind:refreshState={functionRefreshState}
    >
      <svelte:fragment>
        <!-- This is the header for the function picker -->
        <span class="text-base font-medium">Filter functions</span>
      </svelte:fragment>

      <svelte:fragment slot="none-option">No function filter.</svelte:fragment>
    </FunctionPicker>
  </div>
</div>
