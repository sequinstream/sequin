<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Button } from "$lib/components/ui/button";
  import {
    ChevronDown,
    ArrowRightToLine,
    ArrowLeftFromLine,
    CirclePlay,
    CircleStop,
  } from "lucide-svelte";
  import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuTrigger,
  } from "$lib/components/ui/dropdown-menu";
  import { formatRelativeTimestamp } from "$lib/utils";
  // import { Input } from "$lib/components/ui/input";
  // import { Label } from "$lib/components/ui/label";
  // import {
  //   Dialog,
  //   DialogContent,
  //   DialogHeader,
  //   DialogTitle,
  //   DialogTrigger,
  // } from "$lib/components/ui/dialog";

  export let consumers: Array<{
    id: string;
    name: string;
    insertedAt: string;
    type: "pull" | "push";
    status: "active" | "disabled";
    database_name: string;
  }>;
  // export let formErrors;
  export let live: any;

  let newConsumerName = "";
  let dialogOpen = false;

  function handleConsumerClick(id: string) {
    live.pushEvent("consumer_clicked", { id });
  }

  function handleSubmit() {
    live.pushEvent("consumer_submitted", { name: newConsumerName });
    newConsumerName = "";
    dialogOpen = false;
  }
</script>

<div class="container mx-auto py-10">
  <div class="flex justify-between items-center mb-4">
    <h1 class="text-2xl font-bold">Consumers</h1>
    <div class="relative inline-block text-left">
      <div class="inline-flex rounded-md shadow-sm">
        <a
          href="/consumers/new"
          data-phx-link="redirect"
          data-phx-link-state="push"
        >
          <Button class="rounded-r-none" variant="default"
            >Create Consumer</Button
          >
        </a>
        <DropdownMenu>
          <DropdownMenuTrigger asChild let:builder>
            <Button
              variant="default"
              builders={[builder]}
              class="px-2 rounded-l-none border-l border-primary/20"
            >
              <ChevronDown class="h-4 w-4" />
              <span class="sr-only">Open options</span>
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <a
              href="/consumers/new?kind=push"
              data-phx-link="redirect"
              data-phx-link-state="push"
            >
              <DropdownMenuItem class="cursor-pointer">
                Quick create Push Consumer
              </DropdownMenuItem>
            </a>
            <a
              href="/consumers/new?kind=pull"
              data-phx-link="redirect"
              data-phx-link-state="push"
            >
              <DropdownMenuItem class="cursor-pointer">
                Quick create Pull Consumer
              </DropdownMenuItem>
            </a>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </div>
  </div>

  <Table.Root>
    <Table.Header>
      <Table.Row>
        <Table.Head>Name</Table.Head>
        <Table.Head>Database</Table.Head>
        <Table.Head>Type</Table.Head>
        <Table.Head>Status</Table.Head>
        <Table.Head>Created at</Table.Head>
      </Table.Row>
    </Table.Header>
    <Table.Body>
      {#each consumers as consumer}
        <Table.Row
          on:click={() => handleConsumerClick(consumer.id)}
          class="cursor-pointer"
        >
          <Table.Cell>{consumer.name}</Table.Cell>
          <Table.Cell>{consumer.database_name}</Table.Cell>
          <Table.Cell>
            <div class="flex items-center">
              <span
                class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium"
                class:bg-green-50={consumer.type === "pull"}
                class:text-green-400={consumer.type === "pull"}
                class:bg-blue-50={consumer.type === "push"}
                class:text-blue-400={consumer.type === "push"}
              >
                {#if consumer.type === "pull"}
                  <ArrowLeftFromLine class="h-4 w-4 mr-1 text-green-400" />
                {:else if consumer.type === "push"}
                  <ArrowRightToLine class="h-4 w-4 mr-1 text-blue-400" />
                {/if}
                {consumer.type[0].toUpperCase() + consumer.type.slice(1)} consumer
              </span>
            </div>
          </Table.Cell>
          <Table.Cell>
            <span
              class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium"
              class:text-purple-500={consumer.status === "active"}
              class:text-gray-400={consumer.status === "disabled"}
            >
              {#if consumer.status === "active"}
                <CirclePlay class="h-4 w-4 mr-1" />
              {:else if consumer.status === "disabled"}
                <CircleStop class="h-4 w-4 mr-1" />
              {/if}
              {consumer.status[0].toUpperCase() + consumer.status.slice(1)}
            </span>
          </Table.Cell>
          <Table.Cell>{formatRelativeTimestamp(consumer.insertedAt)}</Table.Cell
          >
        </Table.Row>
      {/each}
    </Table.Body>
  </Table.Root>
</div>
