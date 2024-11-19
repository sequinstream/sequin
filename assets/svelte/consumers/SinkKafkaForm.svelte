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
  let internalUsername = form.sink.username || "";
  let internalPassword = form.sink.password || "";

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
  $: if (form.sink.sasl_mechanism !== null) {
    form.sink.username = internalUsername;
    form.sink.password = internalPassword;
  } else {
    form.sink.username = null;
    form.sink.password = null;
  }

  // Function to prepare and emit form data
  function handleSubmit() {
    const payload = {
      ...form.sink,
      // Only include username and password if SASL is enabled
      ...(form.sink.sasl_mechanism !== "none" && {
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
        bind:value={form.sink.hosts}
        placeholder="localhost:9092,localhost:9093"
      />
      <p class="text-sm text-muted-foreground">
        Comma-separated list of host:port pairs
      </p>
      {#if errors.sink?.hosts}
        <p class="text-destructive text-sm">{errors.sink.hosts}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="topic">Topic</Label>
      <Input id="topic" bind:value={form.sink.topic} placeholder="my-topic" />
      {#if errors.sink?.topic}
        <p class="text-destructive text-sm">{errors.sink.topic}</p>
      {/if}
    </div>

    <!-- SASL Mechanism Dropdown -->
    <div class="space-y-2">
      <Label for="sasl_mechanism">SASL Mechanism</Label>
      <select
        id="sasl_mechanism"
        bind:value={form.sink.sasl_mechanism}
        class="block w-full border border-gray-300 rounded-md p-2"
      >
        {#each saslOptions as option}
          <option value={option.value}>{option.label}</option>
        {/each}
      </select>
      {#if errors.sink?.sasl_mechanism}
        <p class="text-destructive text-sm">
          {errors.sink.sasl_mechanism}
        </p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="username">Username</Label>
      <div hidden={form.sink.sasl_mechanism === null}>
        <Input
          id="username"
          bind:value={internalUsername}
          placeholder="Username"
          data-1p-ignore
          autocomplete="off"
        />
      </div>
      <div hidden={form.sink.sasl_mechanism !== null}>
        <Input id="username" placeholder="Username requires SASL" disabled />
      </div>
      {#if errors.sink?.username}
        <p class="text-destructive text-sm">{errors.sink.username}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="password">Password</Label>
      <div class="relative">
        <div hidden={form.sink.sasl_mechanism === null}>
          <Input
            id="password"
            type={showPassword ? "text" : "password"}
            bind:value={internalPassword}
            placeholder="Password"
            data-1p-ignore
            autocomplete="off"
          />
        </div>
        <div hidden={form.sink.sasl_mechanism !== null}>
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
      {#if errors.sink?.password}
        <p class="text-destructive text-sm">{errors.sink.password}</p>
      {/if}
    </div>

    <!-- TLS Switch -->
    <div class="space-y-2">
      <div class="flex items-center gap-2">
        <Switch
          id="tls"
          checked={form.sink.tls}
          onCheckedChange={(checked) => {
            form.sink.tls = checked;
          }}
        />
        <Label for="tls">TLS</Label>
      </div>
      {#if errors.sink?.tls}
        <p class="text-destructive text-sm">{errors.sink.tls}</p>
      {/if}
    </div>
  </CardContent>
</Card>
