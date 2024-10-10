<script lang="ts">
  import { isNavCollapsed } from "../stores/Sidenav";
  import { Button } from "./ui/button";
  import * as Command from "$lib/components/ui/command";
  import * as Popover from "$lib/components/ui/popover";
  import * as Dialog from "$lib/components/ui/dialog";
  import { Label } from "$lib/components/ui/label";
  import { Input } from "$lib/components/ui/input";
  import { cn } from "$lib/utils";
  import {
    Radio,
    Database,
    Webhook,
    FileText,
    LifeBuoy,
    LogOut,
    ChevronLeft,
    ChevronRight,
    Check,
    PlusCircle,
    Cog,
    Loader2,
    Logs,
  } from "lucide-svelte";

  export let live;
  export let currentPath: string;
  export let currentUser: { id: string; email: string };
  export let currentAccountId: string;
  export let accountList: { id: string; name: string }[];
  export let parent: string;

  let settingsCommandOpen = false;
  let form = { name: "" };
  let showCreateAccount = false;
  let createAccountLoading = false;
  let createAccountError: string | null = null;

  const navItems = [
    { path: "/consumers", text: "Consumers", icon: Radio },
    { path: "/databases", text: "Databases", icon: Database },
    { path: "/http-endpoints", text: "HTTP Endpoints", icon: Webhook },
    { path: "/wal-pipelines", text: "WAL Pipelines", icon: Logs },
  ];

  function navLink(path: string) {
    return currentPath.startsWith(path)
      ? "bg-secondary-3xSubtle text-info hover:bg-secondary-2xSubtle"
      : "text-muted hover:text-basis hover:bg-canvasSubtle";
  }

  function toggleCollapse() {
    $isNavCollapsed = !$isNavCollapsed;
  }

  $: selected_account_name =
    accountList.find((a) => a.id === currentAccountId)?.name ??
    "Select an account...";

  function handleAccountSelect(accountId: string) {
    live.pushEventTo(`#${parent}`, "change_selected_account", { accountId });
  }

  function handleCreateAccount(event: Event) {
    event.preventDefault();
    createAccountLoading = true;
    live.pushEventTo(
      `#${parent}`,
      "create_account",
      {
        accountName: form.name,
      },
      (res: any) => {
        createAccountLoading = false;
        if (res.error) {
          createAccountError = res.error;
        } else {
          showCreateAccount = false;
        }
      }
    );
  }

  function openCreateAccount() {
    showCreateAccount = true;
    settingsCommandOpen = false;
  }
</script>

<nav
  class="bg-canvasBase top-0 flex h-screen flex-col justify-start {$isNavCollapsed
    ? 'w-[64px]'
    : 'w-[224px]'} sticky z-[49] shrink-0 overflow-visible border-r border-solid transition-all duration-300"
>
  <div class="mx-2 mt-4 flex h-[28px] flex-row items-center justify-between">
    <a
      href="/"
      class="flex items-center {$isNavCollapsed ? 'ml-2' : 'ml-2'}"
      data-phx-link="redirect"
      data-phx-link-state="push"
    >
      {#if $isNavCollapsed}
        <span class="text-xl font-semibold text-primary">S</span>
      {:else}
        <span class="text-xl font-semibold text-primary">Sequin</span>
      {/if}
    </a>
    <Button
      variant="ghost"
      size="sm"
      class="h-8 p-1.5 {$isNavCollapsed ? 'ml-1' : ''}"
      on:click={toggleCollapse}
    >
      {#if $isNavCollapsed}
        <ChevronRight class="h-4 w-4" />
      {:else}
        <ChevronLeft class="h-4 w-4" />
      {/if}
    </Button>
  </div>

  <div class="flex grow flex-col justify-between">
    <div class="text-basis mx-4 mt-5 flex h-full flex-col">
      {#each navItems as item}
        <a href={item.path} data-phx-link="redirect" data-phx-link-state="push">
          <div
            class="my-1 flex h-8 w-full flex-row items-center rounded px-1.5 {navLink(
              item.path
            )}"
          >
            <svelte:component
              this={item.icon}
              class="h-4 w-4 flex-shrink-0 {$isNavCollapsed ? '' : 'mr-2'}"
            />
            {#if !$isNavCollapsed}
              <span class="text-sm leading-tight truncate">{item.text}</span>
            {/if}
          </div>
        </a>
      {/each}
    </div>

    <div class="mx-4 mb-4">
      <a
        href="https://sequinstream.com/docs"
        target="_blank"
        rel="noopener noreferrer"
      >
        <div
          class="my-1 flex h-8 w-full flex-row items-center rounded px-1.5 {navLink(
            'https://sequinstream.com/docs'
          )}"
        >
          <FileText
            class="h-4 w-4 flex-shrink-0 {$isNavCollapsed ? '' : 'mr-2'}"
          />
          {#if !$isNavCollapsed}
            <span class="text-sm leading-tight truncate">Docs</span>
          {/if}
        </div>
      </a>
      <a id="launch-intercom" href="mailto:support@sequinstream.com">
        <div
          class="my-1 flex h-8 w-full flex-row items-center rounded px-1.5 {navLink(
            'mailto:support@sequinstream.com'
          )}"
        >
          <LifeBuoy
            class="h-4 w-4 flex-shrink-0 {$isNavCollapsed ? '' : 'mr-2'}"
          />
          {#if !$isNavCollapsed}
            <span class="text-sm leading-tight truncate">Support</span>
          {/if}
        </div>
      </a>
      <div class="border-t border-solid my-2"></div>
      <Popover.Root bind:open={settingsCommandOpen}>
        <Popover.Trigger asChild let:builder class="w-full">
          {#if !$isNavCollapsed}
            <Button
              builders={[builder]}
              variant="menu"
              size="menu"
              role="combobox"
              aria-expanded={settingsCommandOpen}
            >
              <Cog class="h-4 w-4 flex-shrink-0 mr-2" />
              <span class="text-sm leading-tight truncate"
                >{selected_account_name}</span
              >
            </Button>
          {:else}
            <Button
              builders={[builder]}
              variant="menu"
              size="menu"
              role="combobox"
              aria-expanded={settingsCommandOpen}
            >
              <Cog class=" h-4 w-4 flex-shrink-0" />
            </Button>
          {/if}
        </Popover.Trigger>
        <Popover.Content class="w-fit p-0" side="right">
          <Command.Root>
            <Command.Group heading="Switch account">
              {#each accountList as account}
                <Command.Item
                  value={account.id}
                  onSelect={() => handleAccountSelect(account.id)}
                >
                  <Check
                    class={cn(
                      "mr-2 h-4 w-4",
                      currentAccountId !== account.id && "text-transparent"
                    )}
                  />
                  {account.name}
                </Command.Item>
              {/each}
            </Command.Group>
            <Command.Separator />
            <Command.Group heading="Account settings">
              <Command.Item
                onSelect={() => openCreateAccount()}
                class="cursor-pointer"
              >
                <PlusCircle class="mr-2 h-4 w-4" />
                <span>Create account</span>
              </Command.Item>
              <a
                href="/settings/accounts"
                data-phx-link="redirect"
                data-phx-link-state="push"
              >
                <Command.Item class="cursor-pointer">
                  <Cog class="mr-2 h-4 w-4" />
                  <span>Manage account</span>
                </Command.Item>
              </a>
            </Command.Group>
            <Command.Separator />
            <Command.Group heading={currentUser.email}>
              <a
                href="/logout"
                data-phx-link="redirect"
                data-phx-link-state="push"
              >
                <Command.Item class="cursor-pointer">
                  <LogOut class="mr-2 h-4 w-4" />
                  Log out
                </Command.Item>
              </a>
            </Command.Group>
          </Command.Root>
        </Popover.Content>
      </Popover.Root>
    </div>
  </div>
</nav>

<Dialog.Root bind:open={showCreateAccount}>
  <Dialog.Content>
    <Dialog.Header>
      <Dialog.Title>Create a new account</Dialog.Title>
    </Dialog.Header>
    <form on:submit={handleCreateAccount}>
      <div class="grid gap-4 py-4 my-4">
        <div class="grid grid-cols-4 items-center gap-4">
          <Label for="name" class="text-right">Account name</Label>
          <Input
            id="name"
            bind:value={form.name}
            placeholder="My new project"
            class="col-span-3"
          />
        </div>
        {#if createAccountError}
          <p class="text-destructive text-sm mt-2 mb-4 col-span-4">
            {createAccountError}
          </p>
        {/if}
      </div>
      <Dialog.Footer>
        <Button variant="outline" on:click={() => (showCreateAccount = false)}>
          Cancel
        </Button>
        <Button
          variant="default"
          type="submit"
          disabled={createAccountLoading}
          loading={createAccountLoading}
        >
          Create
        </Button>
      </Dialog.Footer>
    </form>
  </Dialog.Content>
</Dialog.Root>
