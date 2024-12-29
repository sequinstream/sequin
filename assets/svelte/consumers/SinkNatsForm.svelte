<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import { EyeIcon, EyeOffIcon } from "lucide-svelte";
  import type { NatsConsumer } from "$lib/types/consumer";
  import FormItem from "$lib/components/ui/form-item.svelte";
  import FormErrorMessage from "$lib/components/ui/form-error-message.svelte";
  import FormToggleVisibilityButton from "$lib/components/ui/form-toggle-visibility-button.svelte";
  export let form: NatsConsumer;
  export let errors: any = {};

  let showPassword = false;

  function togglePasswordVisibility() {
    showPassword = !showPassword;
  }
</script>

<Card>
  <CardContent class="flex flex-col gap-4 pt-6">
    <FormItem>
      <Label for="host">Host</Label>
      <Input id="host" bind:value={form.sink.host} placeholder="localhost" />
      {#if errors.sink?.host}
        <FormErrorMessage>{errors.sink.host}</FormErrorMessage>
      {/if}
    </FormItem>

    <FormItem>
      <Label for="port">Port</Label>
      <Input
        id="port"
        type="number"
        bind:value={form.sink.port}
        placeholder="4222"
      />
      {#if errors.sink?.port}
        <FormErrorMessage>{errors.sink.port}</FormErrorMessage>
      {/if}
    </FormItem>

    <FormItem>
      <Label for="username">Username</Label>
      <Input
        id="username"
        bind:value={form.sink.username}
        placeholder="(optional)"
      />
      {#if errors.sink?.username}
        <FormErrorMessage>{errors.sink.username}</FormErrorMessage>
      {/if}
    </FormItem>

    <FormItem>
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
        <FormToggleVisibilityButton
          isVisible={showPassword}
          label="password"
          onToggleVisibility={togglePasswordVisibility}
        />
      </div>
      {#if errors.sink?.password}
        <FormErrorMessage>{errors.sink.password}</FormErrorMessage>
      {/if}
    </FormItem>
  </CardContent>
</Card>
