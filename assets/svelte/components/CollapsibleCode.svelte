<script lang="ts">
  export let code: string;
  export let label: string = "Show code";
  export let threshold: number = 5;
</script>

<div class="mt-2">
  {#if code.split("\n").length > threshold}
    <details>
      <summary class="cursor-pointer text-sm text-muted-foreground mb-2">
        {label}
      </summary>
      <pre
        class="font-mono bg-slate-50 p-2 border border-slate-100 rounded-md text-sm overflow-x-auto"><code
          >{code}</code
        ></pre>
    </details>
  {:else}
    <pre
      class="font-mono bg-slate-50 p-2 border border-slate-100 rounded-md text-sm overflow-x-auto"><code
        >{code}</code
      ></pre>
  {/if}
</div>
