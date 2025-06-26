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
  import { Eye, EyeOff, Info } from "lucide-svelte";
  import DynamicRoutingForm from "$lib/consumers/DynamicRoutingForm.svelte";

  export let form;
  export let errors: any = {};
  export let functions: Array<any> = [];
  export let refreshFunctions: () => void;
  export let functionRefreshState: "idle" | "refreshing" | "done" = "idle";

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
    { value: "aws_msk_iam", label: "AWS MSK IAM" },
  ];

  // Add internal state for AWS credentials
  let internalAwsKeyId = form.sink.aws_access_key_id || "";
  let internalAwsSecretKey = form.sink.aws_secret_access_key || "";
  let internalAwsRegion = form.sink.aws_region || "";

  function togglePasswordVisibility() {
    showPassword = !showPassword;
  }

  // Updated reactive statement to handle credentials
  $: {
    if (form.sink.sasl_mechanism === null) {
      form.sink.username = null;
      form.sink.password = null;
      form.sink.aws_access_key_id = null;
      form.sink.aws_secret_access_key = null;
      form.sink.aws_region = null;
    } else if (form.sink.sasl_mechanism === "aws_msk_iam") {
      form.sink.username = null;
      form.sink.password = null;
      form.sink.aws_access_key_id = internalAwsKeyId;
      form.sink.aws_secret_access_key = internalAwsSecretKey;
      form.sink.aws_region = internalAwsRegion;
    } else {
      form.sink.username = internalUsername;
      form.sink.password = internalPassword;
      form.sink.aws_access_key_id = null;
      form.sink.aws_secret_access_key = null;
      form.sink.aws_region = null;
    }
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

    <DynamicRoutingForm
      {form}
      {functions}
      {refreshFunctions}
      bind:functionRefreshState
      routedSinkType="kafka"
      {errors}
    />

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

    <!-- Username/Password fields -->
    <div
      class="space-y-2"
      hidden={form.sink.sasl_mechanism === null ||
        form.sink.sasl_mechanism === "aws_msk_iam"}
    >
      <Label for="username">Username</Label>
      <Input
        id="username-input"
        bind:value={internalUsername}
        placeholder="Username"
        data-1p-ignore
        autocomplete="off"
      />
      {#if errors.sink?.username}
        <p class="text-destructive text-sm">{errors.sink.username}</p>
      {/if}
    </div>

    <div
      class="space-y-2"
      hidden={form.sink.sasl_mechanism === null ||
        form.sink.sasl_mechanism === "aws_msk_iam"}
    >
      <Label for="password">Password</Label>
      <div class="relative">
        <Input
          id="password-input"
          type={showPassword ? "text" : "password"}
          bind:value={internalPassword}
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

    <!-- AWS Credentials fields -->
    <div class="space-y-2" hidden={form.sink.sasl_mechanism !== "aws_msk_iam"}>
      <Label for="aws_region">AWS Region</Label>
      <Input
        id="aws_region"
        bind:value={internalAwsRegion}
        placeholder="us-east-1"
        data-1p-ignore
        autocomplete="off"
      />
      {#if errors.sink?.aws_region}
        <p class="text-destructive text-sm">{errors.sink.aws_region}</p>
      {/if}
    </div>

    <div class="space-y-2" hidden={form.sink.sasl_mechanism !== "aws_msk_iam"}>
      <Label for="aws_key_id">AWS Access Key ID</Label>
      <Input
        id="aws_key_id"
        bind:value={internalAwsKeyId}
        placeholder="AWS Access Key ID"
        data-1p-ignore
        autocomplete="off"
      />
      {#if errors.sink?.aws_access_key_id}
        <p class="text-destructive text-sm">{errors.sink.aws_access_key_id}</p>
      {/if}
    </div>

    <div class="space-y-2" hidden={form.sink.sasl_mechanism !== "aws_msk_iam"}>
      <Label for="aws_secret_key">AWS Secret Access Key</Label>
      <div class="relative">
        <Input
          id="aws_secret_key"
          type={showPassword ? "text" : "password"}
          bind:value={internalAwsSecretKey}
          placeholder="AWS Secret Access Key"
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
      {#if errors.sink?.aws_secret_access_key}
        <p class="text-destructive text-sm">
          {errors.sink.aws_secret_access_key}
        </p>
      {/if}
    </div>

    <!-- Batch Size field -->
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
        Number of messages to batch together in a single request to Kafka. We
        recommend starting with a batch size of at least 10. You might try
        experimenting with even larger batch sizes to see if it improves
        throughput.
      </p>
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
