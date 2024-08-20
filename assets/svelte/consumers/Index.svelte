<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import {
    Dialog,
    DialogContent,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
  } from "$lib/components/ui/dialog";

  export let consumers: Array<{
    id: string;
    name: string;
    insertedAt: string;
    type: "pull" | "push";
    status: "active" | "disabled";
  }>;
  export let formErrors: Record<string, string>;
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
    <a
      href="/consumers/new"
      data-phx-link="redirect"
      data-phx-link-state="push"
    >
      <Button>Create Consumer</Button>
    </a>
  </div>

  <Table.Root>
    <Table.Header>
      <Table.Row>
        <Table.Head>Name</Table.Head>
        <Table.Head>Type</Table.Head>
        <Table.Head>Status</Table.Head>
        <Table.Head>Created At</Table.Head>
      </Table.Row>
    </Table.Header>
    <Table.Body>
      {#each consumers as consumer}
        <Table.Row
          on:click={() => handleConsumerClick(consumer.id)}
          class="cursor-pointer"
        >
          <Table.Cell>{consumer.name}</Table.Cell>
          <Table.Cell>{consumer.type}</Table.Cell>
          <Table.Cell>{consumer.status}</Table.Cell>
          <Table.Cell
            >{new Date(consumer.insertedAt).toLocaleString()}</Table.Cell
          >
        </Table.Row>
      {/each}
    </Table.Body>
  </Table.Root>
</div>
