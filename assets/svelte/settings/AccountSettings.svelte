<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import * as Card from "$lib/components/ui/card";
  import * as Select from "$lib/components/ui/select";
  import { Input } from "$lib/components/ui/input";
  import * as Dialog from "$lib/components/ui/dialog";
  import { Cog } from "lucide-svelte";
  import { Label } from "$lib/components/ui/label";

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
  let showInviteMember = false;
  let inviteMemberLoading = false;
  let inviteMemberError: string | null = null;
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
          deleteErrorMessage = res.error;
        } else {
          showDeleteConfirmDialog = false;
        }
      }
    );
  }

  function handleInviteMember(event: Event) {
    event.preventDefault();
    inviteMemberLoading = true;
    inviteMemberError = null;
    live.pushEventTo(
      `#${parent}`,
      "invite_member",
      {
        accountId: selectedAccount.id,
        email: form.email,
      },
      (res: any) => {
        res;
        inviteMemberLoading = false;
        if (res.error) {
          inviteMemberError = res.error;
        } else {
          showInviteMember = false;
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

  <Card.Root class="mb-6">
    <Card.Header>
      <Card.Title>Rename Account</Card.Title>
    </Card.Header>
    <Card.Content>
      {#if renameErrorMessage}
        <p class="text-destructive text-sm mt-2 mb-4">{renameErrorMessage}</p>
      {/if}
      <form
        on:submit={handleRenameAccount}
        class="flex flex-col sm:flex-row sm:items-center sm:space-x-4 space-y-4 sm:space-y-0"
      >
        <Input type="text" bind:value={form.name} class="flex-grow" />
        <Button
          variant="default"
          type="submit"
          disabled={renameDisabled}
          loading={renameLoading}
          class="w-full sm:w-auto"
        >
          Rename
        </Button>
      </form>
    </Card.Content>
  </Card.Root>

  <Card.Root class="mb-6">
    <Card.Header>
      <Card.Title>Invite your team</Card.Title>
    </Card.Header>
    <Card.Content>
      <div
        class="flex flex-col sm:flex-row sm:items-center sm:justify-between space-y-4 sm:space-y-0 sm:space-x-4"
      >
        <p>
          Invite your team to your account to allow them to access your Sequin
          resources.
        </p>
        <Button
          on:click={() => (showInviteMember = true)}
          class="w-full sm:w-auto"
        >
          Add team member
        </Button>
      </div>
    </Card.Content>
  </Card.Root>

  <Card.Root>
    <Card.Header>
      <Card.Title>Delete this account</Card.Title>
    </Card.Header>
    <Card.Content>
      <div
        class="flex flex-col sm:flex-row sm:items-center sm:justify-between space-y-4 sm:space-y-0 sm:space-x-4"
      >
        <p>
          Permanently delete this account ({selectedAccount.name}). There is no
          going back.
        </p>
        <Button
          variant="destructive"
          on:click={() => (showDeleteConfirmDialog = true)}
          class="w-full sm:w-auto"
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

<Dialog.Root bind:open={showInviteMember}>
  <Dialog.Content>
    <Dialog.Header>
      <Dialog.Title>Invite a team member</Dialog.Title>
    </Dialog.Header>
    <form on:submit={handleInviteMember}>
      <div class="grid gap-4 py-4 my-4">
        <div class="grid grid-cols-4 items-center gap-4">
          <Label for="email" class="text-right">Email</Label>
          <Input
            id="email"
            bind:value={form.email}
            placeholder="Email"
            class="col-span-3"
          />
        </div>
        {#if inviteMemberError}
          <p class="text-destructive text-sm mt-2 mb-4 col-span-4">
            {inviteMemberError}
          </p>
        {/if}
      </div>
      <Dialog.Footer>
        <Button variant="outline" on:click={() => (showInviteMember = false)}>
          Cancel
        </Button>
        <Button
          variant="default"
          type="submit"
          disabled={inviteMemberLoading}
          loading={inviteMemberLoading}
        >
          Invite
        </Button>
      </Dialog.Footer>
    </form>
  </Dialog.Content>
</Dialog.Root>
