<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
  } from "$lib/components/ui/dialog";
  import { Label } from "$lib/components/ui/label";
  import { RadioGroup, RadioGroupItem } from "$lib/components/ui/radio-group";
  import { ChevronRight } from "lucide-svelte";

  let selectedDestination = "";

  const destinations = [
    { id: "sqs", name: "Amazon SQS" },
    { id: "kinesis", name: "Amazon Kinesis" },
    { id: "kafka", name: "Apache Kafka" },
    { id: "pubsub", name: "Google Cloud Pub/Sub" },
    { id: "eventhubs", name: "Azure Event Hubs" },
    { id: "rabbitmq", name: "RabbitMQ" },
  ];

  function handleSubmit() {
    // Handle the submission logic here
    console.log("Selected destination:", selectedDestination);
  }
</script>

<Dialog>
  <DialogTrigger asChild>
    <Button variant="outline">Select Destination</Button>
  </DialogTrigger>
  <DialogContent class="sm:max-w-[425px]">
    <DialogHeader>
      <DialogTitle>Choose Destination</DialogTitle>
      <DialogDescription>
        Select where you'd like to write your data. You can change this later in
        settings.
      </DialogDescription>
    </DialogHeader>
    <RadioGroup
      bind:value={selectedDestination}
      class="grid grid-cols-2 gap-4 py-4"
    >
      {#each destinations as dest}
        <Label
          for={dest.id}
          class="flex flex-col items-center justify-between rounded-md border-2 border-muted bg-popover p-4 hover:bg-accent hover:text-accent-foreground [&:has([data-state=checked])]:border-primary"
        >
          <RadioGroupItem value={dest.id} id={dest.id} class="sr-only" />
          <svg
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            stroke-width="2"
            stroke-linecap="round"
            stroke-linejoin="round"
            class="mb-3 h-6 w-6"
          >
            <rect width="20" height="14" x="2" y="5" rx="2" />
            <path d="M2 10h20" />
          </svg>
          {dest.name}
        </Label>
      {/each}
    </RadioGroup>
    <DialogFooter>
      <Button
        type="submit"
        class="w-full"
        disabled={!selectedDestination}
        on:click={handleSubmit}
      >
        Confirm Selection
        <ChevronRight class="ml-2 h-4 w-4" />
      </Button>
    </DialogFooter>
  </DialogContent>
</Dialog>
