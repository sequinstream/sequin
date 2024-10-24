<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import * as Card from "$lib/components/ui/card";
  import * as Select from "$lib/components/ui/select";
  import { Input } from "$lib/components/ui/input";
  import * as Dialog from "$lib/components/ui/dialog";
  import { Trash, Cog, Plus, Ellipsis, UserMinus } from "lucide-svelte";
  import * as Table from "$lib/components/ui/table";
  import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuTrigger,
  } from "$lib/components/ui/dropdown-menu";
  import { Label } from "$lib/components/ui/label";

  interface Account {
    id: string;
    name: string;
  }

  interface User {
    id: string;
    name: string;
    email: string;
  }

  interface PendingInvite {
    id: string;
    sent_to: string;
  }

  export let accounts: Account[];
  export let pendingInvites: PendingInvite[];
  export let selectedAccount: Account;
  export let currentAccountUsers: User[];
  export let currentUser: User;

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

  let showRemoveUserDialog = false;
  let removeUserLoading = false;
  let removeUserError: string | null = null;
  let userToRemove: User | null = null;

  $: renameDisabled = selectedAccount.name === form.name;

  function openInviteMemberDialog() {
    showInviteMember = true;
  }

  function closeInviteMemberDialog() {
    showInviteMember = false;
  }

  function handleAccountSelect(accountId: string) {
    live.pushEventTo(`#${parent}`, "change_selected_account", {
      accountId: accountId,
    });
  }

  function handleRevokeInvite(inviteId: string) {
    live.pushEventTo(
      `#${parent}`,
      "revoke_invite",
      {
        inviteId: inviteId,
        accountId: selectedAccount.id,
      },
      (res: { error?: string }) => {
        // TODO: toaster error or open dialog?!
      },
    );
  }

  function handleRenameAccount(event: Event) {
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
      },
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
      },
    );
  }

  function handleInviteMember(event: SubmitEvent) {
    inviteMemberLoading = true;
    inviteMemberError = null;

    live.pushEventTo(
      `#${parent}`,
      "invite_user",
      {
        accountId: selectedAccount.id,
        email: (event.target as HTMLFormElement).email.value,
      },
      (res?: { error?: string }) => {
        inviteMemberLoading = false;
        if (res?.error) {
          inviteMemberError = res.error;
        } else {
          closeInviteMemberDialog();
        }
      },
    );
  }

  function openRemoveUserDialog(user: User) {
    userToRemove = user;
    showRemoveUserDialog = true;
  }

  function closeRemoveUserDialog() {
    showRemoveUserDialog = false;
    userToRemove = null;
  }

  function handleRemoveUser() {
    if (!userToRemove) return;
    removeUserLoading = true;
    removeUserError = null;

    live.pushEventTo(
      `#${parent}`,
      "remove_user",
      {
        accountId: selectedAccount.id,
        userId: userToRemove.id,
      },
      (res?: { error?: string }) => {
        removeUserLoading = false;
        if (res?.error) {
          removeUserError = res.error;
        } else {
          closeRemoveUserDialog();
        }
      },
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

  <div class="flex flex-col gap-6 container w-auto">
    <Card.Root>
      <Card.Header>
        <Card.Title>Rename account</Card.Title>
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

    <div class="bg-card text-card-foreground rounded-lg border shadow-sm">
      <div class="flex p-6 justify-between">
        <h3 class="text-lg font-semibold leading-none tracking-tight">
          Account users
        </h3>
      </div>

      <Table.Root>
        <Table.Header>
          <Table.Row>
            <Table.Head>Name</Table.Head>
            <Table.Head>Email</Table.Head>
            <Table.Head />
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {#each currentAccountUsers as user}
            <Table.Row>
              <Table.Cell>{user.name || "N/A"}</Table.Cell>
              <Table.Cell>
                <div class="flex items-center gap-2">
                  {user.email}
                  {#if user.id === currentUser.id}
                    <span
                      class="text-xs text-muted bg-gray-200 rounded-full px-3 py-1"
                      >You</span
                    >
                  {/if}
                </div>
              </Table.Cell>
              <Table.Cell class="flex justify-end">
                <DropdownMenu>
                  <DropdownMenuTrigger asChild let:builder>
                    <Button variant="ghost" builders={[builder]}>
                      <Ellipsis class="h-4 w-4" />
                      <span class="sr-only"
                        >Account User Menu for {user.email}</span
                      >
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="end">
                    <DropdownMenuItem
                      class="cursor-pointer flex gap-2 items-center"
                      on:click={() => openRemoveUserDialog(user)}
                    >
                      <UserMinus class="size-4" />
                      Remove user
                    </DropdownMenuItem>
                  </DropdownMenuContent>
                </DropdownMenu>
              </Table.Cell>
            </Table.Row>
          {/each}
        </Table.Body>
      </Table.Root>
    </div>

    <div class="bg-card text-card-foreground rounded-lg border shadow-sm">
      <div class="flex p-6 justify-between">
        <h3 class="text-lg font-semibold leading-none tracking-tight">
          Invitations
        </h3>
        {#if pendingInvites.length}
          <Button on:click={openInviteMemberDialog}>
            <Plus class="h-4 w-4 mr-2" />
            Invite user
          </Button>
        {/if}
      </div>

      {#if !pendingInvites.length}
        <div
          class="flex flex-col gap-4 items-center text-center py-12 mx-auto my-auto px-6"
        >
          <h2 class="text-xl font-semibold">No pending invites</h2>
          <p class="text-gray-600">
            Pending invites are users that have been invited to the account but
            have not yet accepted the invite.
          </p>
          <Button
            class="flex items-center gap-2"
            on:click={openInviteMemberDialog}
          >
            <Plus class="h-4 w-4" />
            Invite user
          </Button>
        </div>
      {:else}
        <Table.Root>
          <Table.Header>
            <Table.Row>
              <Table.Head>Email</Table.Head>
              <Table.Head />
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {#each pendingInvites as invite}
              <Table.Row>
                <Table.Cell>
                  <div class="flex items-center gap-2">
                    {invite.sent_to}
                    {#if invite.sent_to === currentUser.email}
                      <span
                        class="text-xs text-muted bg-gray-200 rounded-full px-3 py-1"
                        >You</span
                      >
                    {/if}
                  </div>
                </Table.Cell>
                <Table.Cell class="flex justify-end">
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild let:builder>
                      <Button variant="ghost" builders={[builder]}>
                        <Ellipsis class="h-4 w-4" />
                        <span class="sr-only"
                          >Invite Menu for {invite.sent_to}</span
                        >
                      </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end">
                      <DropdownMenuItem
                        class="cursor-pointer flex gap-2 items-center"
                        on:click={() => handleRevokeInvite(invite.id)}
                      >
                        <Trash class="size-4" />
                        Revoke invite
                      </DropdownMenuItem>
                    </DropdownMenuContent>
                  </DropdownMenu>
                </Table.Cell>
              </Table.Row>
            {/each}
          </Table.Body>
        </Table.Root>
      {/if}
    </div>

    <Card.Root>
      <Card.Header>
        <Card.Title>Delete this account</Card.Title>
      </Card.Header>
      <Card.Content>
        <div class="flex items-center justify-between space-x-4">
          <p>
            Permanently delete this account ({selectedAccount.name}). There is
            no going back.
          </p>
          <Button
            variant="destructive"
            on:click={() => (showDeleteConfirmDialog = true)}
          >
            Delete account
          </Button>
        </div>
      </Card.Content>
    </Card.Root>
  </div>
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
      <Dialog.Title>Invite user to {selectedAccount.name}</Dialog.Title>
    </Dialog.Header>
    <form on:submit|preventDefault={handleInviteMember}>
      <div class="grid gap-4 py-4 my-4">
        <div class="grid grid-cols-4 items-center gap-4">
          <Label for="email" class="text-right">Email</Label>
          <Input id="email" placeholder="Email" class="col-span-3" />
        </div>
        {#if inviteMemberError}
          <p class="text-destructive text-sm mt-2 mb-4 col-span-4">
            {inviteMemberError}
          </p>
        {/if}
      </div>
      <Dialog.Footer>
        <Button variant="outline" on:click={closeInviteMemberDialog}>
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

<Dialog.Root bind:open={showRemoveUserDialog}>
  <Dialog.Content>
    <Dialog.Header>
      <Dialog.Title>Remove user from {selectedAccount.name}</Dialog.Title>
      <Dialog.Description>
        Are you sure you want to remove {userToRemove?.name ||
          userToRemove?.email} from this account?
      </Dialog.Description>
    </Dialog.Header>
    {#if removeUserError}
      <p class="text-destructive text-sm mt-2 mb-4">{removeUserError}</p>
    {/if}
    <Dialog.Footer>
      <Button variant="outline" on:click={closeRemoveUserDialog}>Cancel</Button>
      <Button
        variant="destructive"
        on:click={handleRemoveUser}
        disabled={removeUserLoading}
        loading={removeUserLoading}
      >
        Remove
      </Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
