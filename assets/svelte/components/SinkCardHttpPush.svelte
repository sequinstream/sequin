<script lang="ts">
  import { ExternalLink } from "lucide-svelte";
  import { Card, CardContent } from "$lib/components/ui/card";
  import { concatenateUrl } from "../databases/utils";
  import { Button } from "$lib/components/ui/button";
  import type { HttpPushConsumer } from "../consumers/types";

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
        <a
          href={`/http-endpoints/${consumer.sink.http_endpoint.id}`}
          data-phx-link="redirect"
          data-phx-link-state="push"
        >
          <Button variant="outline" size="sm">
            <ExternalLink class="h-4 w-4 mr-2" />
            View Endpoint
          </Button>
        </a>
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

    <div class="space-y-4 grid xl:grid-cols-3">
      <div class="xl:col-span-3">
        <span class="text-sm text-gray-500">Base URL</span>
        <div class="mt-2 overflow-x-auto overflow-y-hidden">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md"
            >{consumer.sink.http_endpoint.url}</span
          >
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">HTTP Method</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md"
          >
            {#if consumer.routing}
              Determined by <a
                href={`/functions/${consumer.routing.id}`}
                data-phx-link="redirect"
                data-phx-link-state="push"
                class="underline">router</a
              >
              <ExternalLink class="h-4 w-4 inline" />
            {:else}
              POST
            {/if}
          </span>
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Path</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md"
          >
            {#if consumer.routing_id}
              Determined by <a
                href={`/functions/${consumer.routing_id}`}
                data-phx-link="redirect"
                data-phx-link-state="push"
                class="underline">router</a
              >
              <ExternalLink class="h-4 w-4 inline" />
            {:else}
              {consumer.sink.http_endpoint_path}
            {/if}
          </span>
        </div>
      </div>

      {#if !consumer.routing_id}
        <div class="xl:col-span-3">
          <span class="text-sm text-gray-500">Full Endpoint URL</span>
          <div class="mt-2 overflow-x-auto overflow-y-hidden">
            <span
              class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md"
              >{fullEndpointUrl}</span
            >
          </div>
        </div>
      {/if}
    </div>

    {#if consumer.routing}
      <div class="mt-4">
        <span class="text-sm text-gray-500">Router</span>
        <div class="mt-2">
          <pre
            class="font-mono bg-slate-50 p-2 border border-slate-100 rounded-md text-sm overflow-x-auto"><code
              >{consumer.routing.function.code}</code
            ></pre>
        </div>
      </div>
    {/if}
  </CardContent>
</Card>
