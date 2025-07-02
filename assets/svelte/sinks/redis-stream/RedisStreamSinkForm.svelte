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
  import {
    Accordion,
    AccordionContent,
    AccordionItem,
    AccordionTrigger,
  } from "$lib/components/ui/accordion";
  import { Eye, EyeOff } from "lucide-svelte";
  import DynamicRoutingForm from "$lib/consumers/DynamicRoutingForm.svelte";

  export let form;
  export let errors: any = {};
  export let functions: Array<any> = [];
  export let refreshFunctions: () => void;
  export let functionRefreshState: "idle" | "refreshing" | "done" = "idle";
  let selectedDynamic = form.routingMode === "dynamic";
  let showPassword = false;

  function togglePasswordVisibility() {
    showPassword = !showPassword;
  }
</script>

<Card>
  <CardHeader>
    <CardTitle>Redis Configuration</CardTitle>
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
        placeholder="6379"
      />
      {#if errors.sink?.port}
        <p class="text-destructive text-sm">{errors.sink.port}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="username">Username</Label>
      <Input
        id="username"
        bind:value={form.sink.username}
        placeholder="(optional)"
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

    <div class="space-y-2">
      <Label for="database">Database</Label>
      <Input
        id="database"
        type="number"
        bind:value={form.sink.database}
        placeholder="0"
      />
      {#if errors.sink?.database}
        <p class="text-destructive text-sm">{errors.sink.database}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="batchSize">Batch Size</Label>
      <Input
        id="batchSize"
        type="number"
        min="1"
        bind:value={form.batchSize}
        placeholder="10"
      />
      {#if errors.batch_size}
        <p class="text-destructive text-sm">{errors.batch_size}</p>
      {/if}
      <p class="text-xs">
        Number of messages to batch together in a single pipeline to Redis. We
        recommend starting with a batch size of 10. You might try experimenting
        with larger or smaller batch sizes to see if it improves throughput.
      </p>
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
      routedSinkType="redis_stream"
      {functions}
      {refreshFunctions}
      bind:functionRefreshState
      bind:selectedDynamic
      {errors}
    />

    {#if !selectedDynamic}
      <div class="space-y-2">
        <Label for="streamKey">Stream Key</Label>
        <Input
          id="streamKey"
          bind:value={form.sink.streamKey}
          placeholder="my-stream"
        />
        <p class="text-sm text-muted-foreground">
          The key to use for the stream. Records are <code>XADD</code>ed to this
          key.
        </p>
        {#if errors.sink?.stream_key}
          <p class="text-destructive text-sm">{errors.sink.stream_key}</p>
        {/if}
      </div>
    {/if}
  </CardContent>
</Card>
