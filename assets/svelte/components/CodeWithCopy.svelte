<script lang="ts">
  import CopyIcon from "./CopyIcon.svelte";

  export let language: string = "";
  export let code: string = "";
  export let maxWidth: string = "100%";
  export let copyIconPosition: "center" | "top" = "center";

  $: codeContent = code.trim();
</script>

<div
  class="flex flex-row bg-muted p-4 rounded-md overflow-hidden"
  style="max-width: {maxWidth}"
>
  <pre class="text-sm overflow-x-auto flex flex-grow"><code class={language}
      >{codeContent}</code
    >
  </pre>
  <span
    class={`ml-4 flex-shrink-0 ${copyIconPosition === "top" ? "self-start" : "self-center"}`}
  >
    <CopyIcon content={codeContent} />
  </span>
</div>
