<script lang="ts">
  import { isNavCollapsed } from "../stores/Sidenav";
  import { Button } from "./ui/button";
  import * as DropdownMenu from "./ui/dropdown-menu";
  import {
    Radio,
    Database,
    Webhook,
    FileText,
    LifeBuoy,
    CircleUserRound,
    LogOut,
    ChevronLeft,
    ChevronRight,
  } from "lucide-svelte";

  export let currentPath: string;

  const navItems = [
    { path: "/consumers", text: "Consumers", icon: Radio },
    { path: "/databases", text: "Databases", icon: Database },
    { path: "/http-endpoints", text: "HTTP Endpoints", icon: Webhook },
  ];

  function navLink(path: string) {
    return currentPath.startsWith(path)
      ? "bg-secondary-3xSubtle text-info hover:bg-secondary-2xSubtle"
      : "text-muted hover:text-basis hover:bg-canvasSubtle";
  }

  function toggleCollapse() {
    $isNavCollapsed = !$isNavCollapsed;
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
      <a href="mailto:support@sequinstream.com">
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
      <DropdownMenu.Root>
        <DropdownMenu.Trigger class="w-full cursor-pointer ring-0 p-0">
          <div
            class="flex w-full flex-row items-center rounded p-1 justify-start hover:bg-canvasSubtle text-muted"
          >
            <CircleUserRound class="h-4 w-4 flex-shrink-0" />
            {#if !$isNavCollapsed}
              <div
                class="ml-2 flex flex-col items-start justify-start overflow-hidden"
              >
                <div class="text-muted leading-1 text-sm truncate w-full">
                  Paul Mu'adib
                </div>
                <div class="text-subtle text-xs leading-4 truncate w-full">
                  Sequin
                </div>
              </div>
            {/if}
          </div>
        </DropdownMenu.Trigger>
        <DropdownMenu.Content>
          <DropdownMenu.Item>
            <LogOut class="mr-2 h-4 w-4 flex-shrink-0" />
            <span class="truncate">Log out</span>
          </DropdownMenu.Item>
        </DropdownMenu.Content>
      </DropdownMenu.Root>
    </div>
  </div>
</nav>
