<script lang="ts">
  import { Switch } from "$lib/components/ui/switch";
  import { Label } from "$lib/components/ui/label";
  import FunctionPicker from "$lib/consumers/FunctionPicker.svelte";
  import { routedSinkDocs } from "$lib/consumers/dynamicRoutingDocs";
  import type { RoutedSinkType } from "$lib/consumers/types";

  export let form: any;
  export let functions: Array<any> = [];
  export let refreshFunctions: () => void;
  export let functionRefreshState: "idle" | "refreshing" | "done" = "idle";
  export let routedSinkType: RoutedSinkType;
  export let errors: any = {};
  export let selectedDynamic = form.routingMode === "dynamic";
  let routingConfig = routedSinkDocs[routedSinkType];

  // Reactive function to get sink values
  $: getSinkValue = (param: string) => {
    const field = routingConfig.fields[param];
    const formFieldName = field.staticFormField || param;
    const value = form.sink?.[formFieldName];

    return value !== undefined && value !== null && value !== ""
      ? value
      : field.staticValue;
  };

  $: {
    if (selectedDynamic) {
      form.routingMode = "dynamic";
      // Clear routing config fields that have formFieldName when dynamic routing is enabled
      Object.entries(routingConfig.fields).forEach(([param, field]) => {
        if (field.staticFormField && form.sink) {
          form.sink[field.staticFormField] = undefined;
        }
      });
    } else {
      form.routingMode = "static";
      form.routingId = "none";
    }
  }
</script>

<div class="space-y-4">
  <div class="flex items-center space-x-2">
    <Switch id="dynamic-routing" bind:checked={selectedDynamic} />
    <Label for="dynamic-routing">Routing function</Label>
  </div>

  {#if selectedDynamic}
    <div class="p-4 bg-muted/50 rounded-md">
      <FunctionPicker
        {functions}
        selectedFunctionId={form.routingId || "none"}
        title="Routing function"
        onFunctionChange={(functionId) =>
          (form.routingId = functionId === "none" ? null : functionId)}
        {refreshFunctions}
        allowedFunctionTypes={["routing"]}
        typeLabelKey="sink_type"
        bind:refreshState={functionRefreshState}
        {routedSinkType}
      >
        <p class="text-sm text-muted-foreground">
          Select a routing transform to dynamically set routing parameters:
        </p>
      </FunctionPicker>

      {#if errors.routing_id}
        <p class="text-destructive text-sm">{errors.routing_id}</p>
      {/if}

      <div class="p-4 bg-muted/50 rounded-md mt-2">
        <p class="text-sm text-muted-foreground">
          Using dynamic routing configuration which can <i>override</i> defaults:
        </p>
        <ul class="list-disc pl-5 mt-2 space-y-1 text-sm text-muted-foreground">
          {#each Object.entries(routingConfig.fields) as [param, field]}
            <li>
              <span class="font-mono text-xs px-1 rounded"><b>{param}</b>:</span
              ><span class="text-xs px-1 rounded">{field.description}</span>
              <br />
              <span class="font-mono text-xs px-1 rounded"
                >default: {field.dynamicDefault}</span
              >
            </li>
          {/each}
        </ul>
      </div>
    </div>
  {:else}
    <div class="p-4 bg-muted/50 rounded-md">
      <p class="text-sm text-muted-foreground">
        Using the default routing configuration:
      </p>
      <ul class="list-disc pl-5 mt-2 space-y-1 text-sm text-muted-foreground">
        {#each Object.entries(routingConfig.fields) as [param, field]}
          <li>
            <span class="font-mono text-xs px-1 rounded"><b>{param}</b>:</span
            ><span class="text-xs px-1 rounded">{field.description}</span>
            <br />
            <span class="font-mono text-xs px-1 rounded"
              >value: {field.staticFormField
                ? getSinkValue(param)
                : field.staticValue}</span
            >
          </li>
        {/each}
      </ul>
      <p class="text-sm text-muted-foreground mt-2">
        You can use dynamic routing to customize your routing behavior.
      </p>
    </div>
  {/if}
</div>
