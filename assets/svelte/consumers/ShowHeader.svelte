<script lang="ts">
  import {
    ArrowLeft,
    Clock,
    RefreshCw,
    CirclePlay,
    CircleStop,
    Radio,
  } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { Badge } from "$lib/components/ui/badge";
  import { formatRelativeTimestamp } from "../utils";

  export let consumer;
  export let onEdit;
  export let onDelete;
</script>

<header class="bg-white border-b sticky top-0 z-10">
  <div class="container mx-auto px-4 py-4">
    <div class="flex items-center justify-between">
      <div class="flex items-center space-x-4">
        <a href="/consumers">
          <Button variant="ghost" size="sm">
            <ArrowLeft class="h-4 w-4" />
          </Button>
        </a>
        <div class="flex items-center">
          <Radio class="h-6 w-6 mr-2" />
          <h1 class="text-xl font-semibold">{consumer.name}</h1>
        </div>
        {#if consumer.status === "active"}
          <Badge variant="default"
            ><CirclePlay class="h-4 w-4 mr-1" />Active</Badge
          >
        {:else}
          <Badge variant="disabled"
            ><CircleStop class="h-4 w-4 mr-1" />Disabled</Badge
          >
        {/if}
      </div>
      <div class="flex items-center space-x-4">
        <div
          class="hidden lg:flex flex-col items-left gap-1 text-xs text-gray-500"
        >
          <div class="flex items-center gap-2">
            <Clock class="h-4 w-4" />
            <span>Created {formatRelativeTimestamp(consumer.inserted_at)}</span>
          </div>
          <div class="flex items-center gap-2">
            <RefreshCw class="h-4 w-4" />
            <span>Updated {formatRelativeTimestamp(consumer.updated_at)}</span>
          </div>
        </div>
        <Button variant="outline" size="sm" on:click={onEdit}>Edit</Button>
        <Button
          variant="outline"
          size="sm"
          class="text-red-600 hover:text-red-700"
          on:click={onDelete}
        >
          Delete
        </Button>
      </div>
    </div>
  </div>
</header>
