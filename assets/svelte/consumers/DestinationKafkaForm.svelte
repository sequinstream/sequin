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
    <CardTitle>Kafka Configuration</CardTitle>
  </CardHeader>
  <CardContent class="space-y-4">
    <div class="space-y-2">
      <Label for="hosts">Hosts</Label>
      <Input
        id="hosts"
        bind:value={form.destination.hosts}
        placeholder="localhost:9092,localhost:9093"
      />
      <p class="text-sm text-muted-foreground">
        Comma-separated list of host:port pairs
      </p>
      {#if errors.destination?.hosts}
        <p class="text-destructive text-sm">{errors.destination.hosts}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="topic">Topic</Label>
      <Input
        id="topic"
        bind:value={form.destination.topic}
        placeholder="my-topic"
      />
      {#if errors.destination?.topic}
        <p class="text-destructive text-sm">{errors.destination.topic}</p>
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
      <div class="flex items-center gap-2 hidden">
        <Switch
          id="tls"
          checked={form.destination.tls}
          onCheckedChange={(checked) => {
            form.destination.tls = checked;
          }}
          disabled
        />
        <Label for="tls">TLS</Label>
      </div>
      <div class="rounded-md bg-muted px-4 py-3 text-sm text-muted-foreground">
        <span class="font-medium">Note:</span> Talk to the Sequin team to enable
        TLS for Kafka destinations
      </div>
      {#if errors.destination?.tls}
        <p class="text-destructive text-sm">{errors.destination.tls}</p>
      {/if}
    </div>

    {#if form.destination.tls}
      <div class="space-y-4">
        <div class="space-y-2">
          <Label for="ssl_cert_file">SSL Certificate File</Label>
          <Input
            id="ssl_cert_file"
            bind:value={form.destination.ssl_cert_file}
            placeholder="/path/to/cert.pem"
          />
          {#if errors.destination?.ssl_cert_file}
            <p class="text-destructive text-sm">
              {errors.destination.ssl_cert_file}
            </p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="ssl_key_file">SSL Key File</Label>
          <Input
            id="ssl_key_file"
            bind:value={form.destination.ssl_key_file}
            placeholder="/path/to/key.pem"
          />
          {#if errors.destination?.ssl_key_file}
            <p class="text-destructive text-sm">
              {errors.destination.ssl_key_file}
            </p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="ssl_ca_cert_file">SSL CA Certificate File</Label>
          <Input
            id="ssl_ca_cert_file"
            bind:value={form.destination.ssl_ca_cert_file}
            placeholder="/path/to/ca.pem"
          />
          {#if errors.destination?.ssl_ca_cert_file}
            <p class="text-destructive text-sm">
              {errors.destination.ssl_ca_cert_file}
            </p>
          {/if}
        </div>
      </div>
    {/if}

    <div class="space-y-2">
      <Label for="batch-size">Batch size</Label>
      <div class="flex items-center space-x-2">
        <Input
          id="batch-size"
          type="number"
          bind:value={form.batchSize}
          class="w-24"
          min="1"
          max="1"
          disabled
        />
        <span class="text-sm text-muted-foreground">records</span>
      </div>
      <p class="text-sm text-muted-foreground">
        The number of records to include in each Kafka message. Must be 1.
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
