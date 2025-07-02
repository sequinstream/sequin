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
  import { Textarea } from "$lib/components/ui/textarea";

  export let form;
  export let functions: Array<any> = [];
  export let refreshFunctions: () => void;
  export let functionRefreshState: "idle" | "refreshing" | "done" = "idle";
  export let errors: any = {};
  let isDynamicRouting = form.routingMode === "dynamic";
  let headersString = JSON.stringify(form.sink.headers || {}, null, 2);
  $: form.sink.headers = (() => {
    try {
      return headersString.trim() ? JSON.parse(headersString) : {};
    } catch {
      return {};
    }
  })();
  let showPassword = false;

  function togglePasswordVisibility() {
    showPassword = !showPassword;
  }
</script>

<Card>
  <CardHeader>
    <CardTitle>RabbitMQ configuration</CardTitle>
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
        placeholder="5672"
      />
      {#if errors.sink?.port}
        <p class="text-destructive text-sm">{errors.sink.port}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="virtual_host">Virtual Host</Label>
      <Input
        id="virtual_host"
        bind:value={form.sink.virtual_host}
        placeholder="/"
      />
      {#if errors.sink?.virtual_host}
        <p class="text-destructive text-sm">{errors.sink.virtual_host}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="username">Username</Label>
      <Input
        id="username"
        bind:value={form.sink.username}
        placeholder="(optional)"
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
          placeholder="(optional)"
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

    <div class="flex items-center gap-2">
      <Switch
        id="tls"
        checked={form.sink.tls}
        onCheckedChange={(checked) => {
          form.sink.tls = checked;
        }}
      />
      <Label for="tls">TLS</Label>
      {#if errors.sink?.tls}
        <p class="text-destructive text-sm">{errors.sink.tls}</p>
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
      {functions}
      {refreshFunctions}
      bind:functionRefreshState
      routedSinkType="rabbitmq"
      bind:selectedDynamic={isDynamicRouting}
      {errors}
    />

    {#if !isDynamicRouting}
      <div class="space-y-2">
        <Label for="exchange">Exchange</Label>
        <Input
          id="exchange"
          bind:value={form.sink.exchange}
          placeholder="amq.fanout"
        />
        {#if errors.sink?.exchange}
          <p class="text-destructive text-sm">{errors.sink.exchange}</p>
        {/if}
      </div>

      <div class="space-y-2">
        <Label for="headers">Headers (JSON map)</Label>
        <Textarea
          id="headers"
          bind:value={headersString}
          placeholder="Enter headers as JSON"
        />
        {#if errors.sink?.headers}
          <p class="text-destructive text-sm">{errors.sink.headers}</p>
        {/if}
      </div>
    {/if}
  </CardContent>
</Card>
