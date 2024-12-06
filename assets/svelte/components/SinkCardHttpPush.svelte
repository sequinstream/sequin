<script lang="ts">
  import { ExternalLink } from "lucide-svelte";
  import { Card, CardContent } from "$lib/components/ui/card";
  import { concatenateUrl } from "../databases/utils";
  import { Button } from "$lib/components/ui/button";
  import type { HttpPushConsumer } from "../types/consumer";

  export let consumer: HttpPushConsumer;

  function isWebhookSiteUrl(url: string): boolean {
    return url.startsWith("https://webhook.site/");
  }

  function getWebhookSiteViewUrl(url: string): string {
    const uuid = url.split("/").pop();
    return `https://webhook.site/#!/view/${uuid}`;
  }

  $: fullEndpointUrl = concatenateUrl(
    consumer.sink.http_endpoint.url,
    consumer.sink.http_endpoint_path,
  );
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">HTTP Endpoint</h2>
      <div class="flex space-x-2">
        {#if isWebhookSiteUrl(consumer.sink.http_endpoint.url)}
          <a
            href={getWebhookSiteViewUrl(consumer.sink.http_endpoint.url)}
            target="_blank"
            rel="noopener noreferrer"
          >
            <Button variant="outline" size="sm">
              <ExternalLink class="h-4 w-4 mr-2" />
              View on Webhook.site
            </Button>
          </a>
        {/if}
      </div>
    </div>

    <div class="space-y-4 grid xl:grid-cols-2">
      <div>
        <span class="text-sm text-gray-500">HTTP Endpoint Path</span>
        {#if consumer.sink.http_endpoint_path === "" || consumer.sink.http_endpoint_path === null}
          <div class="mt-2">
            <span
              class="bg-slate-50 px-2 py-1 border border-slate-100 rounded-md"
              >No path configured</span
            >
          </div>
        {:else}
          <div class="mt-2 overflow-x-auto overflow-y-hidden">
            <span
              class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md"
              >{consumer.sink.http_endpoint_path}</span
            >
          </div>
        {/if}
      </div>

      <div>
        <span class="text-sm text-gray-500">Full Endpoint URL</span>
        <div class="mt-2 overflow-x-auto overflow-y-hidden">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md"
            >{fullEndpointUrl}</span
          >
        </div>
      </div>
    </div>
  </CardContent>
</Card>
