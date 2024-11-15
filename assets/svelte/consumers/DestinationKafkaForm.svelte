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

  // Internal state for SASL credentials
  let internalUsername = form.destination.username || "";
  let internalPassword = form.destination.password || "";

  // Local state for SASL mechanism
  let saslOptions = [
    { value: null, label: "None" },
    { value: "plain", label: "Plain" },
    { value: "scram_sha_256", label: "SCRAM-SHA-256" },
    { value: "scram_sha_512", label: "SCRAM-SHA-512" },
  ];

  function togglePasswordVisibility() {
    showPassword = !showPassword;
  }

  // Reactive statement to handle SASL credentials
  $: if (form.destination.sasl_mechanism !== null) {
    form.destination.username = internalUsername;
    form.destination.password = internalPassword;
  } else {
    form.destination.username = null;
    form.destination.password = null;
  }

  // Function to prepare and emit form data
  function handleSubmit() {
    const payload = {
      ...form.destination,
      // Only include username and password if SASL is enabled
      ...(form.destination.sasl_mechanism !== "none" && {
        username: internalUsername,
        password: internalPassword,
      }),
    };
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

    <!-- SASL Mechanism Dropdown -->
    <div class="space-y-2">
      <Label for="sasl_mechanism">SASL Mechanism</Label>
      <select
        id="sasl_mechanism"
        bind:value={form.destination.sasl_mechanism}
        class="block w-full border border-gray-300 rounded-md p-2"
      >
        {#each saslOptions as option}
          <option value={option.value}>{option.label}</option>
        {/each}
      </select>
      {#if errors.destination?.sasl_mechanism}
        <p class="text-destructive text-sm">
          {errors.destination.sasl_mechanism}
        </p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="username">Username</Label>
      <div hidden={form.destination.sasl_mechanism === null}>
        <Input
          id="username"
          bind:value={internalUsername}
          placeholder="Username"
        />
      </div>
      <div hidden={form.destination.sasl_mechanism !== null}>
        <Input id="username" placeholder="Username requires SASL" disabled />
      </div>
      {#if errors.destination?.username}
        <p class="text-destructive text-sm">{errors.destination.username}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="password">Password</Label>
      <div class="relative">
        <div hidden={form.destination.sasl_mechanism === null}>
          <Input
            id="password"
            type={showPassword ? "text" : "password"}
            bind:value={internalPassword}
            placeholder="Password"
          />
        </div>
        <div hidden={form.destination.sasl_mechanism !== null}>
          <Input
            id="password"
            type="password"
            placeholder="Password requires SASL"
            disabled
          />
        </div>
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

    <!-- TLS Switch -->
    <div class="space-y-2">
      <div class="flex items-center gap-2">
        <Switch
          id="tls"
          checked={form.destination.tls}
          onCheckedChange={(checked) => {
            form.destination.tls = checked;
          }}
        />
        <Label for="tls">TLS</Label>
      </div>
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
                Kafka at any time. This helps control the flow of messages and
                prevents overwhelming Kafka.
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
