<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import { Switch } from "$lib/components/ui/switch";
  import * as Alert from "$lib/components/ui/alert";
  import { Info } from "lucide-svelte";

  export let sink: any;
  export let errors: any = {};

  // Initialize use_task_role if not present
  if (sink.use_task_role === undefined) {
    sink.use_task_role = false;
  }

  // Clear credentials when switching to task role
  $: if (sink.use_task_role) {
    sink.access_key_id = "";
    sink.secret_access_key = "";
  }
</script>

<div class="space-y-4">
  <div class="flex items-center space-x-2">
    <Switch id="use-task-role" bind:checked={sink.use_task_role} />
    <Label for="use-task-role">Use Task Role Credentials</Label>
  </div>

  {#if sink.use_task_role}
    <Alert.Root>
      <Info class="h-4 w-4" />
      <Alert.Title>Task Role Authentication</Alert.Title>
      <Alert.Description>
        Sequin will use IAM task role credentials instead of explicit access
        keys. Ensure your task role has the necessary permissions for your AWS
        service.
      </Alert.Description>
    </Alert.Root>
  {:else}
    <div class="space-y-4">
      <div class="space-y-2">
        <Label for="access-key">AWS Access Key ID</Label>
        <Input
          id="access-key"
          data-1p-ignore
          bind:value={sink.access_key_id}
          placeholder="Enter your AWS access key ID"
          autocomplete="off"
        />
        {#if errors.access_key_id}
          <p class="text-destructive text-sm">
            {errors.access_key_id}
          </p>
        {/if}
      </div>

      <div class="space-y-2">
        <Label for="secret-key">AWS Secret Access Key</Label>
        <Input
          id="secret-key"
          data-1p-ignore
          type="password"
          bind:value={sink.secret_access_key}
          placeholder="Enter your AWS secret access key"
          autocomplete="off"
        />
        {#if errors.secret_access_key}
          <p class="text-destructive text-sm">
            {errors.secret_access_key}
          </p>
        {/if}
      </div>
    </div>
  {/if}
</div>
