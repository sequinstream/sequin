<script lang="ts">
  import { BookText, Terminal, Download } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Alert, AlertDescription } from "$lib/components/ui/alert";
  import CopyIcon from "$lib/components/CopyIcon.svelte";
  import {
    Accordion,
    AccordionItem,
    AccordionTrigger,
    AccordionContent,
  } from "$lib/components/ui/accordion";

  export let apiBaseUrl: string = "";
  export let apiKey: string = "";
  export let apiTokens: Array<{
    id: string;
    name: string;
    token: string;
    insertedAt?: string;
  }> = [];

  const serverHost = apiBaseUrl.replace(/^https?:\/\//, "");

  let selectedApiTokenId: string = "default";

  $: if (selectedApiTokenId === "default") {
    apiKey = "YOUR_API_KEY";
  } else if (apiTokens.length > 0) {
    const selected = apiTokens.find((t) => t.id === selectedApiTokenId);
    if (selected && apiKey !== selected.token) {
      apiKey = selected.token;
    }
  }

  function handleApiTokenChange(event: Event) {
    selectedApiTokenId = (event.target as HTMLSelectElement).value;
  }
</script>

<div class="container mx-auto py-6 space-y-6">
  <div class="flex items-center gap-2">
    <Terminal class="h-6 w-6" />
    <h1 class="text-2xl font-bold">Sequin CLI</h1>
  </div>

  <div class="grid gap-6">
    <!-- Download Section -->
    <Card>
      <CardHeader>
        <CardTitle>Download the CLI</CardTitle>
        <CardDescription>
          Get the latest version of the Sequin CLI. See the <a
            href="https://sequinstream.com/docs/reference/cli/overview#sequin-cli"
            target="_blank"
            rel="noopener noreferrer"
            class="underline text-blue-600">CLI documentation</a
          > for more details.
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-4">
        <Accordion class="w-full" type="single" collapsible value="homebrew">
          <AccordionItem value="homebrew">
            <AccordionTrigger>Install with Homebrew</AccordionTrigger>
            <AccordionContent>
              <div class="relative">
                <div class="bg-muted p-4 rounded-md">
                  <code class="text-sm"
                    >brew tap sequinstream/sequin
                    git@github.com:sequinstream/homebrew-sequin<br />brew
                    install sequinstream/sequin/sequin</code
                  >
                </div>
                <Button
                  variant="ghost"
                  size="icon"
                  class="absolute right-2 top-2"
                >
                  <CopyIcon
                    content={`brew tap sequinstream/sequin git@github.com:sequinstream/homebrew-sequin\nbrew install sequinstream/sequin/sequin`}
                  />
                </Button>
              </div>
            </AccordionContent>
          </AccordionItem>
          <AccordionItem value="shell">
            <AccordionTrigger>Install with shell</AccordionTrigger>
            <AccordionContent>
              <div class="relative">
                <div class="bg-muted p-4 rounded-md">
                  <code class="text-sm"
                    >curl -sf
                    https://raw.githubusercontent.com/sequinstream/sequin/main/cli/installer.sh
                    | sh</code
                  >
                </div>
                <Button
                  variant="ghost"
                  size="icon"
                  class="absolute right-2 top-2"
                >
                  <CopyIcon
                    content={`curl -sf https://raw.githubusercontent.com/sequinstream/sequin/main/cli/installer.sh | sh`}
                  />
                </Button>
              </div>
            </AccordionContent>
          </AccordionItem>
          <AccordionItem value="source">
            <AccordionTrigger>Build from source</AccordionTrigger>
            <AccordionContent>
              <div class="relative mb-2">
                <div class="bg-muted p-4 rounded-md">
                  <code class="text-sm"
                    >git clone git@github.com:sequinstream/sequin.git<br />cd
                    cli && make build</code
                  >
                </div>
                <Button
                  variant="ghost"
                  size="icon"
                  class="absolute right-2 top-2"
                >
                  <CopyIcon
                    content={`git clone git@github.com:sequinstream/sequin.git\ncd cli && make build`}
                  />
                </Button>
              </div>
            </AccordionContent>
          </AccordionItem>
        </Accordion>
        <div class="space-y-2">
          <p class="text-sm font-medium">
            Test that the CLI is installed properly
          </p>
          <div class="relative mb-2">
            <div class="relative mb-2">
              <div class="bg-muted p-4 rounded-md">
                <code class="text-sm">sequin --help</code>
              </div>
              <Button
                variant="ghost"
                size="icon"
                class="absolute right-2 top-2"
              >
                <CopyIcon content={`sequin --help`} />
              </Button>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>

    <!-- Setup Context Section -->
    <Card>
      <CardHeader>
        <CardTitle>Set up your Context</CardTitle>
        <CardDescription>
          Configure your CLI with your server host and API key.
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <div class="flex items-center gap-2">
            <p class="text-sm font-medium mb-0">
              Add a context with your API key -
            </p>
            {#if apiTokens.length > 0}
              <select
                class="py-1 text-xs border rounded bg-background border-gray-300 focus:outline-none"
                on:change={handleApiTokenChange}
                bind:value={selectedApiTokenId}
              >
                <option
                  value="default"
                  disabled
                  selected={selectedApiTokenId === "default"}
                  >select an API token</option
                >
                {#each apiTokens as token}
                  <option value={token.id}>{token.name}</option>
                {/each}
              </select>
            {/if}
          </div>
          <p class="text-xs text-muted-foreground">
            You can manage your API tokens from your
            <a
              href="/settings/accounts"
              target="_blank"
              rel="noopener noreferrer"
              class="underline font-mono">account settings</a
            >
          </p>
          <div class="relative">
            <div class="bg-muted p-4 rounded-md">
              <code class="text-sm">
                sequin context add default --hostname {serverHost} --api-token {apiKey}
                --set-default
              </code>
            </div>
            <Button variant="ghost" size="icon" class="absolute right-2 top-2">
              <CopyIcon
                content={`sequin context add default --hostname ${serverHost} --api-token ${apiKey} --set-default`}
              />
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>

    <!-- Config Export Section -->
    <Card>
      <CardHeader>
        <CardTitle>Export your configuration</CardTitle>
        <CardDescription>
          Export your current configuration to a YAML file, then edit and apply
          changes using the CLI.
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <p class="text-sm font-medium">
            Export your configuration in order to edit it according to your
            needs:
          </p>
          <div class="relative">
            <div class="bg-muted p-4 rounded-md">
              <code class="text-sm">sequin config export &gt; sequin.yaml</code>
            </div>
            <Button variant="ghost" size="icon" class="absolute right-2 top-2">
              <CopyIcon content="sequin config export > sequin.yaml" />
            </Button>
          </div>
          <p class="text-sm font-medium mt-4">Preview your changes:</p>
          <div class="relative">
            <div class="bg-muted p-4 rounded-md">
              <code class="text-sm">sequin config plan sequin.yaml</code>
            </div>
            <Button variant="ghost" size="icon" class="absolute right-2 top-2">
              <CopyIcon content="sequin config plan sequin.yaml" />
            </Button>
          </div>
          <p class="text-sm font-medium mt-4">Apply your changes:</p>
          <div class="relative">
            <div class="bg-muted p-4 rounded-md">
              <code class="text-sm">sequin config apply sequin.yaml</code>
            </div>
            <Button variant="ghost" size="icon" class="absolute right-2 top-2">
              <CopyIcon content="sequin config apply sequin.yaml" />
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  </div>
</div>

<!-- Call to Action Button -->
<div class="flex flex-col items-center mt-10">
  <h2 class="text-xl font-semibold mb-4 text-center">
    Discover more Sequin CLI features
  </h2>
  <a
    href="https://sequinstream.com/docs/reference/cli/overview#sequin-cli"
    target="_blank"
    rel="noopener noreferrer"
  >
    <Button variant="outline" size="lg" class="text-lg">
      <BookText class="h-4 w-4 mr-1" />
      Docs
    </Button>
  </a>
</div>
