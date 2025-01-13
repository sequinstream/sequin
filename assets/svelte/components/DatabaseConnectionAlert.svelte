<script lang="ts">
  import { AlertCircle, Database, Zap } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import {
    Alert,
    AlertDescription,
    AlertTitle,
  } from "$lib/components/ui/alert";
  import LinkPushNavigate from "./LinkPatchNavigate.svelte";

  export let show: boolean = false;
  export let entityName: string;
  export let showTryWithLocalDatabaseCTA: boolean = true;
</script>

{#if show}
  <Alert class="bg-carbon-50 border-carbon-200 text-carbon-900 w-full mb-8">
    <div class="grid grid-cols-[auto_1fr] gap-2 items-center">
      <AlertCircle class="h-5 w-5 text-carbon-600" />
      <AlertTitle class="text-lg font-semibold text-carbon-900">
        First, you need to connect to a database
      </AlertTitle>
      <AlertDescription class="text-carbon-600 col-start-2">
        Sequin must be connected to at least one Postgres database before you
        can create a {entityName}.
      </AlertDescription>

      <div class="flex mt-2 gap-4 col-start-2">
        <a
          href="/databases/new"
          data-phx-link="redirect"
          data-phx-link-state="push"
        >
          <Button
            variant="default"
            class="bg-blue-600 text-white border-blue-700 hover:bg-blue-700 hover:text-white transition-colors duration-200 shadow-lg hover:shadow-xl"
          >
            <Database class="inline-block h-4 w-4 mr-2" />
            Connect database
          </Button>
        </a>

        {#if showTryWithLocalDatabaseCTA}
          <LinkPushNavigate href="/databases/new?localhost=true">
            <Button variant="outline">
              <Zap class="inline-block h-4 w-4 mr-2" /> Try with local database
            </Button>
          </LinkPushNavigate>
        {/if}
      </div>
    </div>
  </Alert>
{/if}
