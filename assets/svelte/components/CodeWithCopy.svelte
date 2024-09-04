<script lang="ts">
  import CopyIcon from "./CopyIcon.svelte";

  export let language: string = "";
  export let code: string = "";

  $: codeContent = code.trim();
</script>

<div class="relative bg-muted p-4 rounded-md mb-4">
  <pre class="text-sm overflow-x-auto"><code class={language}
      >{codeContent}</code
    ></pre>
  <div class="absolute top-2 right-2">
    <CopyIcon content={codeContent} />
  </div>
</div>
