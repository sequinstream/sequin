<script lang="ts">
  import FullPageModal from "../components/FullPageModal.svelte";
  import { Button } from "$lib/components/ui/button";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import { PlusCircle, Eye, EyeOff } from "lucide-svelte";
  import {
    Popover,
    PopoverContent,
    PopoverTrigger,
  } from "$lib/components/ui/popover";
  import { Switch } from "$lib/components/ui/switch";
  import { Info } from "lucide-svelte";
  import CodeWithSecret from "$lib/components/CodeWithSecret.svelte";

  export let httpEndpoint: {
    id?: string;
    name: string;
    baseUrl: string;
    headers: Record<string, string>;
    encryptedHeaders: Record<string, string>;
    useLocalTunnel: boolean;
  };
  export let errors: Record<string, any> = {};
  export let parent: string;
  export let live;
  export let api_token: { name: string; token: string };

  const url = httpEndpoint.id ? new URL(httpEndpoint.baseUrl) : ({} as any);
  let baseUrl = httpEndpoint.useLocalTunnel
    ? `${url.pathname}${url.search}${url.hash}`
    : httpEndpoint.baseUrl;

  let form = { ...httpEndpoint, baseUrl };
  let isEdit = !!form.id;
  let dialogOpen = true;
  let showConfirmDialog = false;
  let validating = false;
  let showEncryptedValues: Record<string, boolean> = {};

  // Map.take
  $: baseUrlErrors = Object.fromEntries(
    ["host", "scheme", "port", "path", "query", "fragment"]
      .map((key) => [key, errors[key]])
      .filter(([_, value]) => value !== undefined)
  );

  function pushEvent(
    event: string,
    payload = {},
    callback = (event: any) => {}
  ) {
    live.pushEventTo(`#${parent}`, event, payload, callback);
  }

  function handleSubmit(event: Event) {
    event.preventDefault();
    pushEvent("form_submitted", { form }, (reply) => {
      if (reply?.ok !== true) {
        validating = false;
      }
    });
  }

  $: pushEvent("form_updated", { form });

  function handleClose() {
    pushEvent("form_closed");
  }

  function addHeader() {
    form.headers = { ...form.headers, "": "" };
  }

  function updateHeaderKey(oldKey: string, newKey: string) {
    const { [oldKey]: value, ...rest } = form.headers;
    form.headers = { ...rest, [newKey]: value };
  }

  function updateHeaderValue(key: string, value: string) {
    form.headers = { ...form.headers, [key]: value };
  }

  function removeHeader(key: string) {
    const { [key]: _, ...rest } = form.headers;
    form.headers = rest;
  }

  function addEncryptedHeader() {
    form.encryptedHeaders = { ...form.encryptedHeaders, "": "" };
  }

  function updateEncryptedHeaderKey(oldKey: string, newKey: string) {
    const { [oldKey]: value, ...rest } = form.encryptedHeaders;
    form.encryptedHeaders = { ...rest, [newKey]: value };
  }

  function updateEncryptedHeaderValue(key: string, value: string) {
    form.encryptedHeaders = { ...form.encryptedHeaders, [key]: value };
  }

  function removeEncryptedHeader(key: string) {
    const { [key]: _, ...rest } = form.encryptedHeaders;
    form.encryptedHeaders = rest;
  }

  function toggleEncryptedValue(key: string) {
    showEncryptedValues[key] = !showEncryptedValues[key];
  }

  function toggleLocalTunnel() {
    form.useLocalTunnel = !form.useLocalTunnel;
    pushEvent("form_updated", { form });
  }

  $: setupTunnelCode = `# if it's your first time using the Sequin CLI
sequin context add default --api-token={{secret}} --set-default

# every time you want to boot the tunnel
# Replace [your-local-port] with the local port you want Sequin to connect to
sequin tunnel --ports=[your-local-port]:${form.name}`;
</script>

<FullPageModal
  title={isEdit ? "Edit HTTP Endpoint" : "Create HTTP Endpoint"}
  bind:open={dialogOpen}
  bind:showConfirmDialog
  on:close={handleClose}
>
  <form on:submit={handleSubmit} class="space-y-6 max-w-3xl mx-auto">
    <Card class="mt-12">
      <CardHeader>
        <CardTitle>HTTP Endpoint Configuration</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-4">
          <div class="space-y-2">
            <Label for="name">Name</Label>
            <div class="flex flex-col gap-1">
              <Input
                id="http-endpoint-name"
                bind:value={form.name}
                autocomplete="off"
                data-1p-ignore
                placeholder="Enter a unique name for your HTTP endpoint"
              />
              <p class="text-xs font-light">
                Must contain only alphanumeric characters, underscores, hyphens,
                or dots.
              </p>
            </div>
            {#if errors.name}
              <p class="text-sm text-destructive">{errors.name}</p>
            {/if}
          </div>

          <div class="space-y-2">
            <div
              class="grid grid-cols-[1fr_auto] items-center align-middle gap-2"
            >
              <Label for="baseUrl">
                {form.useLocalTunnel ? "Local path" : "URL"}
              </Label>
              <div class="flex items-center space-x-2 mb-2">
                <Switch
                  id="use-localhost"
                  checked={form.useLocalTunnel}
                  onCheckedChange={toggleLocalTunnel}
                  disabled={isEdit}
                />
                <Label for="use-localhost">Use localhost</Label>
                <Popover>
                  <PopoverTrigger>
                    <Info class="w-4 h-4 text-muted-foreground" />
                  </PopoverTrigger>
                  <PopoverContent>
                    You can use the Sequin CLI to connect Sequin to an HTTP
                    endpoint running on your local machine.
                  </PopoverContent>
                </Popover>
              </div>
            </div>
            {#if form.useLocalTunnel}
              <div class="flex flex-row bg-white">
                <Input
                  id="http-endpoint-baseUrl"
                  bind:value={form.baseUrl}
                  placeholder="/api"
                />
              </div>
              <div
                class="text-sm p-4 text-muted-foreground bg-muted rounded-md"
              >
                <p class="mb-4">
                  Install the Sequin CLI and boot up a tunnel now.
                </p>
                <div>
                  <CodeWithSecret
                    preClass="text-xs bg-muted"
                    containerClass="bg-muted"
                    tabs={[
                      {
                        name: "Connect",
                        value: setupTunnelCode,
                      },
                    ]}
                    secret={api_token.token}
                  />
                </div>
              </div>
            {:else}
              <Input
                id="http-endpoint-baseUrl"
                bind:value={form.baseUrl}
                placeholder="https://api.example.com"
              />
            {/if}

            <ul>
              {#each Object.entries(baseUrlErrors) as [key, value], index}
                <li class="text-sm text-destructive">{key}: {value}</li>
              {/each}
            </ul>

            <div class="space-y-2">
              <Label>Headers</Label>
              {#each Object.entries(form.headers) as [key, value], index}
                <div class="grid grid-cols-[1fr_1fr_15px] gap-4 mb-2">
                  <Input
                    id="http-endpoint-header-key-{index}"
                    value={key}
                    on:input={(e) =>
                      updateHeaderKey(key, e.currentTarget.value)}
                    placeholder="Key"
                  />
                  <Input
                    id="http-endpoint-header-value-{index}"
                    {value}
                    on:input={(e) =>
                      updateHeaderValue(key, e.currentTarget.value)}
                    placeholder="Value"
                  />
                  <button
                    type="button"
                    on:click={() => removeHeader(key)}
                    class="text-muted-foreground hover:text-foreground justify-self-end"
                  >
                    <icon class="hero-x-mark w-4 h-4" />
                  </button>
                </div>
              {/each}
              <div class="grid grid-cols-1 gap-4 max-w-fit">
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  on:click={addHeader}
                  class="mt-2"
                >
                  <PlusCircle class="w-4 h-4 mr-2" />
                  Add Header
                </Button>
              </div>
            </div>

            <div class="space-y-2">
              <Label>Encrypted Headers</Label>
              {#each Object.entries(form.encryptedHeaders) as [key, value], index}
                <div class="grid grid-cols-[1fr_1fr_auto_15px] gap-4 mb-2">
                  <Input
                    id="http-endpoint-encrypted-header-key-{index}"
                    value={key}
                    on:input={(e) =>
                      updateEncryptedHeaderKey(key, e.currentTarget.value)}
                    placeholder="Key"
                  />
                  <Input
                    id="http-endpoint-encrypted-header-value-{index}"
                    {value}
                    on:input={(e) =>
                      updateEncryptedHeaderValue(key, e.currentTarget.value)}
                    placeholder="Value"
                    type={showEncryptedValues[key] ? "text" : "password"}
                  />
                  <button
                    type="button"
                    on:click={() => toggleEncryptedValue(key)}
                    class="text-muted-foreground hover:text-foreground"
                  >
                    {#if showEncryptedValues[key]}
                      <EyeOff class="w-4 h-4" />
                    {:else}
                      <Eye class="w-4 h-4" />
                    {/if}
                  </button>
                  <button
                    type="button"
                    on:click={() => removeEncryptedHeader(key)}
                    class="text-muted-foreground hover:text-foreground justify-self-end"
                  >
                    <icon class="hero-x-mark w-4 h-4" />
                  </button>
                </div>
              {/each}
              <div class="grid grid-cols-1 gap-4 max-w-fit">
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  on:click={addEncryptedHeader}
                  class="mt-2"
                >
                  <PlusCircle class="w-4 h-4 mr-2" />
                  Add Encrypted Header
                </Button>
              </div>
            </div>
          </div>
        </div></CardContent
      >
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>HTTP Endpoint</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <Button type="submit" loading={validating} variant="default">
          <span slot="loading"> Validating... </span>
          {#if isEdit}
            Update HTTP Endpoint
          {:else}
            Create HTTP Endpoint
          {/if}
        </Button>
      </CardContent>
    </Card>
  </form>
</FullPageModal>
