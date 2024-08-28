<script lang="ts">
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
  } from "lucide-svelte";

  export let currentPath: string;

  const navItems = [
    { path: "/consumers", text: "Consumers" },
    { path: "/databases", text: "Databases" },
    { path: "/http-endpoints", text: "HTTP Endpoints" },
  ];

  function navLink(path: string) {
    return currentPath.startsWith(path)
      ? "bg-secondary-3xSubtle text-info hover:bg-secondary-2xSubtle"
      : "text-muted hover:text-basis hover:bg-canvasSubtle";
  }
</script>

<nav
  class="bg-canvasBase top-0 flex h-screen flex-col justify-start w-[224px] sticky z-[49] shrink-0 overflow-visible border-r border-solid"
>
  <div class="mx-4 mt-4 flex h-[28px] flex-row items-center justify-between">
    <a
      href="/"
      class="flex items-center"
      data-phx-link="redirect"
      data-phx-link-state="push"
    >
      <span class="text-xl font-semibold text-primary">Sequin</span>
    </a>
    <Button variant="ghost" size="sm" class="h-8 p-1.5 hidden group-hover:block"
    ></Button>
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
            {#if item.text === "Databases"}
              <Database class="h-4 w-4 mr-2" />
            {:else if item.text === "Consumers"}
              <Radio class="h-4 w-4 mr-2" />
            {:else if item.text === "HTTP Endpoints"}
              <Webhook class="h-4 w-4 mr-2" />
            {/if}
            <span class="text-sm leading-tight">{item.text}</span>
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
          <FileText class="h-4 w-4 mr-2" />
          <span class="text-sm leading-tight">Docs</span>
        </div>
      </a>
      <a href="mailto:support@sequinstream.com">
        <div
          class="my-1 flex h-8 w-full flex-row items-center rounded px-1.5 {navLink(
            'mailto:support@sequinstream.com'
          )}"
        >
          <LifeBuoy class="h-4 w-4 mr-2" />
          <span class="text-sm leading-tight">Support</span>
        </div>
      </a>
      <div class="border-t border-solid my-2"></div>
      <DropdownMenu.Root>
        <DropdownMenu.Trigger class="w-full cursor-pointer ring-0 p-0">
          <div
            class="flex w-full flex-row items-center rounded p-1 justify-start hover:bg-canvasSubtle text-muted"
          >
            <CircleUserRound class="h-4 w-4" />
            <div class="ml-2 flex flex-col items-start justify-start">
              <div class="text-muted leading-1 text-sm">Paul Mu'adib</div>
              <div class="text-subtle text-xs leading-4">Sequin</div>
            </div>
          </div>
        </DropdownMenu.Trigger>
        <DropdownMenu.Content>
          <DropdownMenu.Item>
            <LogOut class="mr-2 h-4 w-4" />
            <span>Log out</span>
          </DropdownMenu.Item>
        </DropdownMenu.Content>
      </DropdownMenu.Root>
    </div>
  </div>
</nav>
