<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import * as Card from "$lib/components/ui/card";
  import { Input } from "$lib/components/ui/input";
  import * as Dialog from "$lib/components/ui/dialog";
  import { Label } from "$lib/components/ui/label";
  import { Info, User } from "lucide-svelte";
  import * as Alert from "$lib/components/ui/alert";

  export let parent: string;
  export let live;
  export let currentUser: {
    id: string;
    email: string;
    auth_provider: string;
  };

  export let selfHosted: boolean;

  let showDeleteConfirmDialog = false;
  let deleteConfirmDialogLoading = false;
  let passwordChangeEnabled = currentUser.auth_provider === "identity";
  let changePasswordLoading = false;
  let currentPassword = "";
  let newPassword = "";
  let newPasswordConfirmation = "";
  let changePasswordErrors: any = {};
  let newEmail = currentUser.email;
  let changeEmailLoading = false;
  let changeEmailErrors: any = {};

  function handleChangePassword(event: SubmitEvent) {
    event.preventDefault();
    changePasswordLoading = true;
    changePasswordErrors = {};

    live.pushEventTo(
      `#${parent}`,
      "change_password",
      {
        current_password: currentPassword,
        new_password: newPassword,
        new_password_confirmation: newPasswordConfirmation,
      },
      (res: any) => {
        changePasswordLoading = false;
        if (res.ok) {
          currentPassword = "";
          newPassword = "";
          newPasswordConfirmation = "";
        } else {
          changePasswordErrors = res.errors;
        }
      },
    );
  }

  function handleDeleteUser() {
    deleteConfirmDialogLoading = true;
    live.pushEventTo(`#${parent}`, "delete_user", {}, (res: any) => {
      deleteConfirmDialogLoading = false;
      if (res.ok) {
        showDeleteConfirmDialog = false;
      }
    });
  }

  function handleUpdateEmail() {
    changeEmailLoading = true;
    changeEmailErrors = {};

    live.pushEventTo(
      `#${parent}`,
      "update_email",
      { email: newEmail },
      (res: any) => {
        changeEmailLoading = false;
        if (res.ok) {
          newEmail = res.email;
        } else {
          changeEmailErrors = res.errors;
        }
      },
    );
  }
</script>

<div>
  <div class="flex items-center mb-4">
    <User class="h-6 w-6 mr-2" />
    <h1 class="text-2xl font-bold">User Settings</h1>
  </div>

  <div class="flex flex-col gap-6 container w-auto">
    <Card.Root>
      <Card.Header>
        <Card.Title>Email address</Card.Title>
      </Card.Header>
      <Card.Content>
        {#if selfHosted}
          {#if currentUser.auth_provider === "github"}
            <Alert.Root variant="warning">
              <Alert.Description>
                <div class="flex items-center gap-2">
                  <Info class="h-4 w-4" />
                  <p>
                    You cannot change your email when using GitHub
                    authentication.
                  </p>
                </div>
              </Alert.Description>
            </Alert.Root>
          {:else}
            <form
              on:submit|preventDefault={handleUpdateEmail}
              class="space-y-4"
            >
              <div class="grid w-full items-center gap-1.5">
                <Label for="email">Email</Label>
                <Input
                  type="email"
                  id="email"
                  bind:value={newEmail}
                  disabled={currentUser.auth_provider === "github"}
                />
                {#if changeEmailErrors.email}
                  <p class="text-sm text-destructive">
                    {changeEmailErrors.email[0]}
                  </p>
                {/if}
              </div>
              <Button
                type="submit"
                disabled={changeEmailLoading ||
                  currentUser.auth_provider === "github"}
                loading={changeEmailLoading}
              >
                Change email
              </Button>
            </form>
          {/if}
        {:else}
          <p class="text-gray-600">{currentUser.email}</p>
        {/if}
      </Card.Content>
    </Card.Root>

    <Card.Root>
      <Card.Header>
        <Card.Title>Change password</Card.Title>
      </Card.Header>
      <Card.Content>
        {#if currentUser.auth_provider === "github"}
          <Alert.Root variant="warning">
            <Alert.Description>
              <div class="flex items-center gap-2">
                <Info class="h-4 w-4" />
                <p>
                  You cannot change your password when using GitHub
                  authentication.
                </p>
              </div>
            </Alert.Description>
          </Alert.Root>
        {/if}
        <form on:submit={handleChangePassword} class="mt-4 space-y-4">
          <div class="grid w-full items-center gap-1.5">
            <Label for="current-password">Current password</Label>
            <Input
              type="password"
              id="current-password"
              bind:value={currentPassword}
              disabled={!passwordChangeEnabled}
            />
            {#if changePasswordErrors.current_password}
              <p class="text-sm text-destructive">
                {changePasswordErrors.current_password[0]}
              </p>
            {/if}
          </div>
          <div class="grid w-full items-center gap-1.5">
            <Label for="new-password">New password</Label>
            <Input
              type="password"
              id="new-password"
              bind:value={newPassword}
              disabled={!passwordChangeEnabled}
            />
            {#if changePasswordErrors.password}
              <p class="text-sm text-destructive">
                {changePasswordErrors.password[0]}
              </p>
            {/if}
          </div>
          <div class="grid w-full items-center gap-1.5">
            <Label for="confirm-password">Confirm new password</Label>
            <Input
              type="password"
              id="confirm-password"
              bind:value={newPasswordConfirmation}
              disabled={!passwordChangeEnabled}
            />
            {#if changePasswordErrors.password_confirmation}
              <p class="text-sm text-destructive">
                {changePasswordErrors.password_confirmation[0]}
              </p>
            {/if}
          </div>
          <Button
            type="submit"
            disabled={changePasswordLoading || !passwordChangeEnabled}
            loading={changePasswordLoading}
          >
            Change password
          </Button>
        </form>
      </Card.Content>
    </Card.Root>

    <!-- <Card.Root>
      <Card.Header>
        <Card.Title>Delete user</Card.Title>
      </Card.Header>
      <Card.Content>
        <div class="flex items-center justify-between space-x-4">
          <p>Permanently delete your user. This action cannot be undone.</p>
          <Button
            variant="destructive"
            on:click={() => (showDeleteConfirmDialog = true)}
          >
            Delete user
          </Button>
        </div>
      </Card.Content>
    </Card.Root> -->
  </div>
</div>

<Dialog.Root bind:open={showDeleteConfirmDialog}>
  <Dialog.Content>
    <Dialog.Header>
      <Dialog.Title>Are you sure you want to delete your account?</Dialog.Title>
      <Dialog.Description>This action cannot be undone.</Dialog.Description>
    </Dialog.Header>
    <Dialog.Footer>
      <Button
        variant="outline"
        on:click={() => (showDeleteConfirmDialog = false)}
      >
        Cancel
      </Button>
      <Button
        variant="destructive"
        on:click={handleDeleteUser}
        disabled={deleteConfirmDialogLoading}
        loading={deleteConfirmDialogLoading}
      >
        Delete
      </Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
