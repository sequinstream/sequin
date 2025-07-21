<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import { Switch } from "$lib/components/ui/switch";
  import { Eye, EyeOff } from "lucide-svelte";
  import DynamicRoutingForm from "$lib/consumers/DynamicRoutingForm.svelte";

  export let form;
  export let errors: any = {};
  export let functions: Array<any> = [];
  export let refreshFunctions: () => void;
  export let functionRefreshState: "idle" | "refreshing" | "done" = "idle";

  let showPassword = false;
  let isDynamicRouting = form.routingMode === "dynamic";

  function togglePasswordVisibility() {
    showPassword = !showPassword;
  }
</script>

<Card>
  <CardHeader>
    <CardTitle>PostgreSQL Configuration</CardTitle>
  </CardHeader>
  <CardContent class="space-y-4">
    <div class="space-y-2">
      <Label for="host">Host</Label>
      <Input id="host" bind:value={form.sink.host} placeholder="localhost" />
      {#if errors.sink?.host}
        <p class="text-destructive text-sm">{errors.sink.host}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="port">Port</Label>
      <Input
        id="port"
        type="number"
        bind:value={form.sink.port}
        placeholder="5432"
      />
      {#if errors.sink?.port}
        <p class="text-destructive text-sm">{errors.sink.port}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="database">Database</Label>
      <Input
        id="database"
        bind:value={form.sink.database}
        placeholder="postgres"
      />
      {#if errors.sink?.database}
        <p class="text-destructive text-sm">{errors.sink.database}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="username">Username</Label>
      <Input
        id="username"
        bind:value={form.sink.username}
        placeholder="postgres"
        data-1p-ignore
        autocomplete="off"
      />
      {#if errors.sink?.username}
        <p class="text-destructive text-sm">{errors.sink.username}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="password">Password</Label>
      <div class="relative">
        <Input
          id="password"
          type={showPassword ? "text" : "password"}
          bind:value={form.sink.password}
          placeholder="Password"
          data-1p-ignore
          autocomplete="off"
        />
        <button
          type="button"
          class="absolute inset-y-0 right-0 flex items-center pr-3"
          on:click={togglePasswordVisibility}
        >
          {#if showPassword}
            <EyeOff class="h-4 w-4 text-gray-400" />
          {:else}
            <Eye class="h-4 w-4 text-gray-400" />
          {/if}
        </button>
      </div>
      {#if errors.sink?.password}
        <p class="text-destructive text-sm">{errors.sink.password}</p>
      {/if}
    </div>

    <!-- SSL Switch -->
    <div class="space-y-2">
      <div class="flex items-center gap-2">
        <Switch
          id="ssl"
          checked={form.sink.ssl}
          onCheckedChange={(checked) => {
            form.sink.ssl = checked;
          }}
        />
        <Label for="ssl">SSL</Label>
      </div>
      {#if errors.sink?.ssl}
        <p class="text-destructive text-sm">{errors.sink.ssl}</p>
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
      {form}
      {functions}
      {refreshFunctions}
      bind:functionRefreshState
      routedSinkType="postgres"
      bind:selectedDynamic={isDynamicRouting}
      {errors}
    />

    {#if !isDynamicRouting}
      <div class="space-y-2">
        <Label for="table_name">Table Name</Label>
        <Input
          id="table_name"
          bind:value={form.sink.table_name}
          placeholder="my_table"
        />
        {#if errors.sink?.table_name}
          <p class="text-destructive text-sm">{errors.sink.table_name}</p>
        {/if}
      </div>
    {/if}
  </CardContent>
</Card>
