<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import * as Card from "$lib/components/ui/card";
  import * as Select from "$lib/components/ui/select";
  import { Input } from "$lib/components/ui/input";
  import * as Dialog from "$lib/components/ui/dialog";
  import { Cog } from "lucide-svelte";

  interface Account {
    id: string;
    name: string;
  }

  export let accounts: Account[];
  export let selectedAccount: Account;

  export let parent: string;
  export let live;

  let form = { ...selectedAccount };
  let renameLoading = false;
  let renameErrorMessage: string | null = null;
  let showDeleteConfirmDialog = false;
  let deleteConfirmDialogLoading = false;
  let deleteErrorMessage: string | null = null;
  $: renameDisabled = selectedAccount.name === form.name;

  function handleAccountSelect(accountId: string) {
    live.pushEventTo(`#${parent}`, "change_selected_account", {
      accountId: accountId,
    });
  }

  function handleRenameAccount(event: Event) {
    event.preventDefault();
    renameLoading = true;
    live.pushEventTo(
      `#${parent}`,
      "rename_account",
      {
        accountId: selectedAccount.id,
        name: form.name,
      },
      (res: any) => {
        renameLoading = false;
        if (res.error) {
          renameErrorMessage = res.error;
        }
      }
    );
  }

  function handleDeleteAccount() {
    deleteConfirmDialogLoading = true;
    deleteErrorMessage = null;
    live.pushEventTo(
      `#${parent}`,
      "delete_account",
      {
        accountId: selectedAccount.id,
      },
      (res: any) => {
        deleteConfirmDialogLoading = false;
        if (res.error) {
          console.error(res.error);
          deleteErrorMessage = res.error;
        } else {
          showDeleteConfirmDialog = false;
        }
      }
    );
  }
</script>

<div>
  <div class="flex justify-between items-center mb-4">
    <div class="flex items-center">
      <Cog class="h-6 w-6 mr-2" />
      <h1 class="text-2xl font-bold">Account Settings</h1>
    </div>
    <Select.Root
      selected={{ value: selectedAccount.id, label: selectedAccount.name }}
      onSelectedChange={(event) => handleAccountSelect(event.value)}
    >
      <Select.Trigger id="account-select" class="w-[200px]">
        <Select.Value>{selectedAccount.name}</Select.Value>
      </Select.Trigger>
      <Select.Content>
        {#each accounts as account}
          <Select.Item value={account.id}>{account.name}</Select.Item>
        {/each}
      </Select.Content>
    </Select.Root>
  </div>

  <div class="w-full rounded-lg border-2 border-dashed border-gray-300 mb-6">
    <div class="text-center py-12 w-1/2 mx-auto my-auto">
      <h2 class="text-xl font-semibold mb-4">Invite your team</h2>
      <p class="text-gray-600 mb-6">
        Invite your team to your account to allow them to access your Sequin
        resources. Coming by the end of October 2024 - send us a note if you
        want this right away.
      </p>
      <a
        href="/http-endpoints/new"
        data-phx-link="redirect"
        data-phx-link-state="push"
      >
        <Button disabled>Add a team member</Button>
      </a>
    </div>
  </div>

  <Card.Root class="mb-6">
    <Card.Header>
      <Card.Title>Rename Account</Card.Title>
    </Card.Header>
    <Card.Content>
      {#if renameErrorMessage}
        <p class="text-destructive text-sm mt-2 mb-4">{renameErrorMessage}</p>
      {/if}
      <form on:submit={handleRenameAccount} class="flex space-x-4">
        <Input type="text" bind:value={form.name} />
        <Button
          variant="default"
          type="submit"
          disabled={renameDisabled}
          loading={renameLoading}
        >
          Rename
        </Button>
      </form>
    </Card.Content>
  </Card.Root>

  <Card.Root>
    <Card.Header>
      <Card.Title>Delete this account</Card.Title>
    </Card.Header>
    <Card.Content>
      <div class="flex items-center justify-between space-x-4">
        <p>
          Permanently delete this account ({selectedAccount.name}). There is no
          going back.
        </p>
        <Button
          variant="destructive"
          on:click={() => (showDeleteConfirmDialog = true)}
        >
          Delete Account
        </Button>
      </div>
    </Card.Content>
  </Card.Root>
</div>

<Dialog.Root bind:open={showDeleteConfirmDialog}>
  <Dialog.Content>
    <Dialog.Header>
      <Dialog.Title>Are you sure you want to delete this Account?</Dialog.Title>
      <Dialog.Description>This action cannot be undone.</Dialog.Description>
    </Dialog.Header>
    {#if deleteErrorMessage}
      <p class="text-destructive text-sm mt-2 mb-4">{deleteErrorMessage}</p>
    {/if}
    <Dialog.Footer>
      <Button
        variant="outline"
        on:click={() => (showDeleteConfirmDialog = false)}
      >
        Cancel
      </Button>
      <Button
        variant="destructive"
        on:click={handleDeleteAccount}
        disabled={deleteConfirmDialogLoading}
        loading={deleteConfirmDialogLoading}
      >
        Delete
      </Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
