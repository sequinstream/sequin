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

  export let form;
  export let errors: any = {};
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
      <Input
        id="host"
        bind:value={form.destination.host}
        placeholder="localhost"
      />
      {#if errors.destination?.host}
        <p class="text-destructive text-sm">{errors.destination.host}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="port">Port</Label>
      <Input
        id="port"
        type="number"
        bind:value={form.destination.port}
        placeholder="6379"
      />
      {#if errors.destination?.port}
        <p class="text-destructive text-sm">{errors.destination.port}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="username">Username</Label>
      <Input
        id="username"
        bind:value={form.destination.username}
        placeholder="(optional)"
      />
      {#if errors.destination?.username}
        <p class="text-destructive text-sm">{errors.destination.username}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="password">Password</Label>
      <div class="relative">
        <Input
          id="password"
          type={showPassword ? "text" : "password"}
          bind:value={form.destination.password}
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
      {#if errors.destination?.password}
        <p class="text-destructive text-sm">{errors.destination.password}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="streamKey">Stream Key</Label>
      <Input
        id="streamKey"
        bind:value={form.destination.streamKey}
        placeholder="my-stream"
      />
      <p class="text-sm text-muted-foreground">
        The key to use for the stream. Records are <code>XADD</code>ed to this
        key.
      </p>
      {#if errors.destination?.stream_key}
        <p class="text-destructive text-sm">{errors.destination.stream_key}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="database">Database</Label>
      <Input
        id="database"
        type="number"
        bind:value={form.destination.database}
        placeholder="0"
      />
      {#if errors.destination?.database}
        <p class="text-destructive text-sm">{errors.destination.database}</p>
      {/if}
    </div>

    <div class="flex items-center gap-2">
      <Switch
        id="tls"
        checked={form.destination.tls}
        onCheckedChange={(checked) => {
          form.destination.tls = checked;
        }}
      />
      <Label for="tls">TLS</Label>
      {#if errors.destination?.tls}
        <p class="text-destructive text-sm">{errors.destination.tls}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="batch-size">Batch size</Label>
      <div class="flex items-center space-x-2">
        <Input
          id="batch-size"
          type="number"
          bind:value={form.batchSize}
          class="w-24"
          min="1"
          max="1000"
        />
        <span class="text-sm text-muted-foreground">records</span>
      </div>
      <p class="text-sm text-muted-foreground">
        The number of records to include in each Redis transaction. Maximum of
        1000.
      </p>
      {#if errors.batch_size}
        <p class="text-destructive text-sm">{errors.batch_size}</p>
      {/if}
    </div>

    <Accordion class="w-full">
      <AccordionItem value="advanced">
        <AccordionTrigger>Advanced configuration</AccordionTrigger>
        <AccordionContent>
          <div class="space-y-4 pt-4">
            <div class="space-y-2">
              <Label for="max-ack-pending">Max ack pending</Label>
              <Input
                id="max-ack-pending"
                type="number"
                bind:value={form.maxAckPending}
              />
              <p class="text-sm text-muted-foreground">
                Sets the maximum number of messages that can be in-flight to
                Redis at any time. This helps control the flow of messages and
                prevents overwhelming Redis.
              </p>
              {#if errors.max_ack_pending}
                <p class="text-destructive text-sm">{errors.max_ack_pending}</p>
              {/if}
            </div>
          </div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  </CardContent>
</Card>
