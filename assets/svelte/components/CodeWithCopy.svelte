<script lang="ts">
  import CopyIcon from "./CopyIcon.svelte";

  export let language: string = "";
  let codeElement: HTMLElement;
  $: codeContent = codeElement?.textContent || "";
</script>

<div class="relative bg-muted p-4 rounded-md mb-4">
  <pre class="text-sm overflow-x-auto"><code
      bind:this={codeElement}
      class={language}><slot /></code
    ></pre>
  <div class="absolute top-2 right-2">
    <CopyIcon content={codeContent} />
  </div>
</div>
