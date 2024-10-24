<script lang="ts">
  import { isNavCollapsed } from "../stores/Sidenav";
  import { Button } from "./ui/button";
  import * as Command from "$lib/components/ui/command";
  import * as Popover from "$lib/components/ui/popover";
  import * as Dialog from "$lib/components/ui/dialog";
  import * as Tooltip from "$lib/components/ui/tooltip";
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
    Logs,
    ListOrdered,
    RefreshCw,
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
    {
      heading: "Stream",
      items: [
        { path: "/sequences", text: "Sequences", icon: ListOrdered },
        { path: "/consumers/pull", text: "Consume Endpoints", icon: Radio },
        {
          path: "/consumers/push",
          text: "Webhook Subscriptions",
          icon: Webhook,
        },
        {
          path: "/sync-endpoints",
          text: "Sync Endpoints",
          icon: RefreshCw,
          disabledText: "Sync Endpoints are coming soon",
        },
      ],
    },
    {
      heading: "Resources",
      items: [
        { path: "/databases", text: "Databases", icon: Database },
        { path: "/wal-pipelines", text: "WAL Pipelines", icon: Logs },
        { path: "/http-endpoints", text: "HTTP Endpoints", icon: Webhook },
      ],
    },
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
      },
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
    : 'w-[224px]'}shrink-0 overflow-visible border-r border-solid transition-all duration-300"
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
      {#each navItems as group}
        {#if !$isNavCollapsed}
          <h3 class="text-xs font-semibold text-muted mb-2 mt-4">
            {group.heading}
          </h3>
        {/if}
        {#each group.items as item}
          {#if item.disabledText}
            <Tooltip.Root>
              <Tooltip.Trigger asChild>
                <div
                  class="my-1 flex h-8 w-full flex-row items-center rounded px-1.5 text-carbon-300 cursor-not-allowed"
                >
                  <svelte:component
                    this={item.icon}
                    class="h-4 w-4 flex-shrink-0 {$isNavCollapsed
                      ? ''
                      : 'mr-2'}"
                  />
                  {#if !$isNavCollapsed}
                    <span class="text-sm leading-tight truncate"
                      >{item.text}</span
                    >
                    <span
                      class="px-2 py-1 rounded-full bg-carbon-100 text-carbon-600 ml-2"
                      style="font-size: 0.5em">SOON</span
                    >
                  {/if}
                </div>
              </Tooltip.Trigger>
              <Tooltip.Content class="max-w-xs">
                <p class="text-xs text-gray-500">{item.disabledText}</p>
              </Tooltip.Content>
            </Tooltip.Root>
          {:else}
            <a
              href={item.path}
              data-phx-link="redirect"
              data-phx-link-state="push"
            >
              <div
                class="my-1 flex h-8 w-full flex-row items-center rounded px-1.5 {navLink(
                  item.path,
                )}"
              >
                <svelte:component
                  this={item.icon}
                  class="h-4 w-4 flex-shrink-0 {$isNavCollapsed ? '' : 'mr-2'}"
                />
                {#if !$isNavCollapsed}
                  <span class="text-sm leading-tight truncate">{item.text}</span
                  >
                {/if}
              </div>
            </a>
          {/if}
        {/each}
      {/each}
    </div>

    <div class="mx-4 mb-4">
      <a
        href="https://discord.gg/BV8wFXvNtY"
        target="_blank"
        rel="noopener noreferrer"
        class="group"
      >
        <div
          class="my-1 flex h-8 w-full flex-row items-center rounded px-1.5 {navLink(
            'https://discord.gg/BV8wFXvNtY',
          )}"
        >
          <svg
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 127.14 96.36"
            class="fill-gray-600 group-hover:fill-gray-800 h-4 w-4 flex-shrink-0 {$isNavCollapsed
              ? ''
              : 'mr-2'}"
            ><path
              d="M107.7,8.07A105.15,105.15,0,0,0,81.47,0a72.06,72.06,0,0,0-3.36,6.83A97.68,97.68,0,0,0,49,6.83,72.37,72.37,0,0,0,45.64,0,105.89,105.89,0,0,0,19.39,8.09C2.79,32.65-1.71,56.6.54,80.21h0A105.73,105.73,0,0,0,32.71,96.36,77.7,77.7,0,0,0,39.6,85.25a68.42,68.42,0,0,1-10.85-5.18c.91-.66,1.8-1.34,2.66-2a75.57,75.57,0,0,0,64.32,0c.87.71,1.76,1.39,2.66,2a68.68,68.68,0,0,1-10.87,5.19,77,77,0,0,0,6.89,11.1A105.25,105.25,0,0,0,126.6,80.22h0C129.24,52.84,122.09,29.11,107.7,8.07ZM42.45,65.69C36.18,65.69,31,60,31,53s5-12.74,11.43-12.74S54,46,53.89,53,48.84,65.69,42.45,65.69Zm42.24,0C78.41,65.69,73.25,60,73.25,53s5-12.74,11.44-12.74S96.23,46,96.12,53,91.08,65.69,84.69,65.69Z"
            /></svg
          >
          {#if !$isNavCollapsed}
            <span class="text-sm leading-tight truncate">Discord</span>
          {/if}
        </div>
      </a>
      <a
        href="https://sequinstream.com/docs"
        target="_blank"
        rel="noopener noreferrer"
      >
        <div
          class="my-1 flex h-8 w-full flex-row items-center rounded px-1.5 {navLink(
            'https://sequinstream.com/docs',
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
            'mailto:support@sequinstream.com',
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
                      currentAccountId !== account.id && "text-transparent",
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
