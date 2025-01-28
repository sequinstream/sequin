<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import { Input } from "$lib/components/ui/input";
  import FormItem from "$lib/components/ui/form-item.svelte";
  import FormErrorMessage from "$lib/components/ui/form-error-message.svelte";
  import FormToggleVisibilityButton from "$lib/components/ui/form-toggle-visibility-button.svelte";

  export let form;
  export let errors;

  let showToken = false;

  function toggleTokenVisibility() {
    showToken = !showToken;
  }
</script>

<Card>
  <CardContent class="flex flex-col gap-4 pt-6">
    <FormItem>
      <Label for="token">Token</Label>
      <div class="relative">
        <Input
          id="token"
          type={showToken ? "text" : "password"}
          bind:value={form.sink.token}
          placeholder="Enter your S2 token"
          autocomplete="off"
        />
        <FormToggleVisibilityButton
          isVisible={showToken}
          label="token"
          onToggleVisibility={toggleTokenVisibility}
        />
      </div>
      {#if errors.sink?.token}
        <FormErrorMessage>{errors.sink.token}</FormErrorMessage>
      {/if}
    </FormItem>

    <FormItem>
      <Label for="basin">Basin</Label>
      <Input
        id="basin"
        bind:value={form.sink.basin}
        placeholder="Enter your basin name"
      />
      {#if errors.sink?.basin}
        <FormErrorMessage>{errors.sink.basin}</FormErrorMessage>
      {/if}
    </FormItem>

    <FormItem>
      <Label for="stream">Stream</Label>
      <Input
        id="stream"
        bind:value={form.sink.stream}
        placeholder="Enter your stream name"
      />
      {#if errors.sink?.stream}
        <FormErrorMessage>{errors.sink.stream}</FormErrorMessage>
      {/if}
    </FormItem>
  </CardContent>
</Card>
