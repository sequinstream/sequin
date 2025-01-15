<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import type { AzureEventHubConsumer } from "$lib/types/consumer";
  import FormItem from "$lib/components/ui/form-item.svelte";
  import FormErrorMessage from "$lib/components/ui/form-error-message.svelte";
  import FormToggleVisibilityButton from "$lib/components/ui/form-toggle-visibility-button.svelte";

  export let form: AzureEventHubConsumer;
  export let errors: any = {};

  let showPassword = false;

  function togglePasswordVisibility() {
    showPassword = !showPassword;
  }
</script>

<Card>
  <CardHeader>
    <CardTitle>Azure EventHub Configuration</CardTitle>
  </CardHeader>
  <CardContent class="flex flex-col gap-4">
    <FormItem>
      <Label for="namespace">Namespace</Label>
      <Input
        id="namespace"
        bind:value={form.sink.namespace}
        placeholder="my-namespace"
      />
      {#if errors.sink?.namespace}
        <FormErrorMessage>{errors.sink.namespace}</FormErrorMessage>
      {/if}
    </FormItem>

    <FormItem>
      <Label for="event-hub-name">Event Hub Name</Label>
      <Input
        id="event-hub-name"
        bind:value={form.sink.event_hub_name}
        placeholder="my-event-hub"
      />
      {#if errors.sink?.event_hub_name}
        <FormErrorMessage>{errors.sink.event_hub_name}</FormErrorMessage>
      {/if}
    </FormItem>

    <FormItem>
      <Label for="shared-access-key-name">Shared Access Key Name</Label>
      <Input
        id="shared-access-key-name"
        bind:value={form.sink.shared_access_key_name}
        placeholder="RootManageSharedAccessKey"
      />
      {#if errors.sink?.shared_access_key_name}
        <FormErrorMessage>{errors.sink.shared_access_key_name}</FormErrorMessage
        >
      {/if}
    </FormItem>

    <FormItem>
      <Label for="shared-access-key">Shared Access Key</Label>
      <div class="relative">
        <Input
          id="shared-access-key"
          type={showPassword ? "text" : "password"}
          bind:value={form.sink.shared_access_key}
          placeholder="Enter your shared access key"
          data-1p-ignore
          autocomplete="off"
        />
        <FormToggleVisibilityButton
          isVisible={showPassword}
          label="shared access key"
          onToggleVisibility={togglePasswordVisibility}
        />
      </div>
      {#if errors.sink?.shared_access_key}
        <FormErrorMessage>{errors.sink.shared_access_key}</FormErrorMessage>
      {/if}
    </FormItem>
  </CardContent>
</Card>
