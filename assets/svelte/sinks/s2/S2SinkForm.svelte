<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import DynamicRoutingForm from "$lib/consumers/DynamicRoutingForm.svelte";

  export let form;
  export let functions: Array<any> = [];
  export let refreshFunctions: () => void;
  export let functionRefreshState: "idle" | "refreshing" | "done" = "idle";
  export let errors: any = {};
  let selectedDynamic = form.routingMode === "dynamic";
</script>

<Card>
  <CardHeader>
    <CardTitle>S2 Configuration</CardTitle>
  </CardHeader>
  <CardContent class="flex flex-col gap-4">
    <div class="flex flex-col gap-2">
      <Label for="access-token">Access Token</Label>
      <Input
        id="access-token"
        data-1p-ignore
        type="password"
        bind:value={form.sink.access_token}
        placeholder="Enter your token"
        autocomplete="off"
      />
      {#if errors.sink?.access_token}
        <p class="text-destructive text-sm">{errors.sink.access_token}</p>
      {/if}
    </div>
  </CardContent>
</Card>

<Card>
  <CardHeader>
    <CardTitle>Routing</CardTitle>
  </CardHeader>
  <CardContent class="space-y-4">
    <DynamicRoutingForm
      bind:form
      bind:selectedDynamic
      {functions}
      {refreshFunctions}
      bind:functionRefreshState
      {errors}
      routedSinkType="s2"
    />

    {#if !selectedDynamic}
      <div class="flex flex-col gap-2">
        <Label for="basin">Basin</Label>
        <Input
          id="basin"
          bind:value={form.sink.basin}
          placeholder="your-basin"
        />
        {#if errors.sink?.basin}
          <p class="text-destructive text-sm">{errors.sink.basin}</p>
        {/if}
      </div>

      <div class="flex flex-col gap-2">
        <Label for="stream">Stream</Label>
        <Input
          id="stream"
          bind:value={form.sink.stream}
          placeholder="my-stream"
        />
        {#if errors.sink?.stream}
          <p class="text-destructive text-sm">{errors.sink.stream}</p>
        {/if}
      </div>
    {/if}
  </CardContent>
</Card>
