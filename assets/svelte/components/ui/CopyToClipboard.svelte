<script lang="ts">
  import { Clipboard, WandSparkles, Check } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { onDestroy } from "svelte";
  import { cn } from "$lib/utils";

  export let textFn: () => string;
  export let buttonText: string = "Copy to clipboard";
  export let successText: string = "Copied to clipboard";
  export let buttonVariant:
    | "default"
    | "outline"
    | "destructive"
    | "ghost"
    | "link"
    | "magic" = "outline";
  export let buttonSize: "default" | "sm" | "lg" | "icon" = "default";
  export let className: string = "";
  let copySuccess = false;
  let copyTimeout: ReturnType<typeof setTimeout>;

  function handleCopy() {
    // Try using the modern Clipboard API first
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard
        .writeText(textFn())
        .then(() => {
          setSuccessState();
        })
        .catch((err) => {
          console.error("Failed to copy text: ", err);
          // Fallback to older method
          fallbackCopyToClipboard(textFn());
        });
    } else {
      // Fallback to older method
      fallbackCopyToClipboard(textFn());
    }
  }

  // Fallback method using document.execCommand
  function fallbackCopyToClipboard(text: string) {
    const textArea = document.createElement("textarea");
    textArea.value = text;

    // Avoid scrolling to bottom
    textArea.style.top = "0";
    textArea.style.left = "0";
    textArea.style.position = "fixed";
    textArea.style.opacity = "0";

    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();

    try {
      const successful = document.execCommand("copy");
      if (successful) {
        setSuccessState();
      }
    } catch (err) {
      console.error("Fallback: Oops, unable to copy", err);
    }

    document.body.removeChild(textArea);
  }

  function setSuccessState() {
    copySuccess = true;
    // Clear any existing timeout
    if (copyTimeout) {
      clearTimeout(copyTimeout);
    }
    // Set new timeout to reset the success state
    copyTimeout = setTimeout(() => {
      copySuccess = false;
    }, 2000);
  }

  // Clean up timeout on component destroy
  onDestroy(() => {
    if (copyTimeout) {
      clearTimeout(copyTimeout);
    }
  });
</script>

<Button
  variant={buttonVariant}
  size={buttonSize}
  class={cn(
    className,
    buttonVariant === "magic" &&
      "relative overflow-hidden group shadow-md shadow-blue-200/30 hover:shadow-lg hover:shadow-blue-200/50 transition-all duration-300",
  )}
  on:click={handleCopy}
>
  {#if copySuccess}
    <Check class="w-4 h-4 mr-2 text-green-500" />
    {successText}
  {:else}
    <WandSparkles
      class={cn(
        "w-4 h-4 mr-2 text-blue-500 animate-pulse",
        buttonVariant === "magic" &&
          "group-hover:text-blue-600 group-hover:animate-pulse group-hover:scale-110 transition-all duration-300",
      )}
    />
    {buttonText}
  {/if}
  {#if buttonVariant === "magic"}
    <div
      class="absolute inset-0 bg-gradient-to-r from-blue-500/10 to-purple-500/10 group-hover:from-blue-500/20 group-hover:to-purple-500/20 transition-all duration-300"
    />
  {/if}
</Button>
