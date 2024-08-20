<script lang="ts">
  import { Button } from "./ui/button";

  export let currentPath: string;

  const navItems = [
    { path: "/consumers", text: "Consumers", icon: "consumers-icon" },
    { path: "/databases", text: "Databases", icon: "databases-icon" },
    {
      path: "/http-endpoints",
      text: "HTTP Endpoints",
      icon: "http-endpoints-icon",
    },
  ];

  const bottomNavItems = [
    { path: "/docs", text: "Docs", icon: "docs-icon" },
    { path: "/support", text: "Support", icon: "support-icon" },
  ];
  function navLink(path: string) {
    return currentPath === path
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
            <span class="ml-2.5 text-sm leading-tight">{item.text}</span>
          </div>
        </a>
      {/each}
    </div>

    <div class="mx-4 mb-4">
      {#each bottomNavItems as item}
        <a href={item.path} data-phx-link="redirect" data-phx-link-state="push">
          <div
            class="my-1 flex h-8 w-full flex-row items-center rounded px-1.5 {navLink(
              item.path
            )}"
          >
            <span class="ml-2.5 text-sm leading-tight">{item.text}</span>
          </div>
        </a>
      {/each}
      <div class="border-t border-solid mt-2"></div>
      <Button variant="ghost" class="w-full cursor-pointer ring-0">
        <div
          class="flex h-16 w-full flex-row items-center justify-start px-2.5"
        >
          <div
            class="flex w-full flex-row items-center rounded p-1 justify-start hover:bg-canvasSubtle text-muted"
          >
            <div
              class="bg-canvasMuted text-muted flex h-8 w-8 items-center justify-center rounded-full text-xs uppercase"
            >
              se
            </div>
            <div class="ml-2 flex flex-col items-start justify-start">
              <div class="text-muted leading-1 text-sm">sequin</div>
              <div class="text-subtle text-xs leading-4">Paul Mu'adib</div>
            </div>
          </div>
        </div>
      </Button>
    </div>
  </div>
</nav>
