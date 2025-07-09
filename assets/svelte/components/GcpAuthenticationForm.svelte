<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import { Switch } from "$lib/components/ui/switch";
  import { Textarea } from "$lib/components/ui/textarea";
  import * as Alert from "$lib/components/ui/alert";
  import { Info } from "lucide-svelte";

  export let sink: any;
  export let errors: any = {};
  export let isSelfHosted: boolean = true;

  // Initialize use_application_default_credentials if not present
  if (sink.use_application_default_credentials === undefined) {
    sink.use_application_default_credentials = false;
  }

  // Force to false when not self-hosted
  $: if (!isSelfHosted && sink.use_application_default_credentials) {
    sink.use_application_default_credentials = false;
  }

  // Clear credentials when switching to application default
  $: if (sink.use_application_default_credentials) {
    sink.credentials = "";
  }
</script>

<div class="space-y-4">
  <div class="flex items-center space-x-2">
    <Switch
      id="use-application-default-credentials"
      bind:checked={sink.use_application_default_credentials}
      disabled={!isSelfHosted}
    />
    <Label for="use-application-default-credentials"
      >Use Application Default Credentials</Label
    >
  </div>

  {#if !isSelfHosted}
    <Alert.Root>
      <Info class="h-4 w-4" />
      <Alert.Title>Cloud Mode</Alert.Title>
      <Alert.Description>
        Application Default Credentials are not supported in Sequin Cloud for
        security reasons. Please use explicit service account credentials
        instead.
      </Alert.Description>
    </Alert.Root>
  {/if}

  {#if sink.use_application_default_credentials}
    <Alert.Root>
      <Info class="h-4 w-4" />
      <Alert.Title>Application Default Credentials</Alert.Title>
      <Alert.Description>
        Sequin will automatically discover GCP credentials from the environment
        instead of requiring explicit service account keys. This includes:
        <ul class="list-disc list-inside mt-2 space-y-1">
          <li>GOOGLE_APPLICATION_CREDENTIALS environment variable</li>
          <li>gcloud application default credentials</li>
          <li>Compute Engine/Cloud Run metadata service</li>
        </ul>
      </Alert.Description>
    </Alert.Root>
  {:else}
    <div class="space-y-4">
      <div class="space-y-2">
        <Label for="credentials">Service Account Key (JSON)</Label>
        <Textarea
          id="credentials"
          data-1p-ignore
          bind:value={sink.credentials}
          placeholder="Paste your service account key JSON here..."
          autocomplete="off"
          class="min-h-[120px] font-mono text-sm"
        />
        {#if errors.credentials}
          <p class="text-destructive text-sm">
            {errors.credentials}
          </p>
        {/if}
      </div>
    </div>
  {/if}
</div>
