<script>
  import { Button } from "$lib/components/ui/button";
  export let text = "Click me!";
  export let count = 0;

  function handleClick() {
    count += 1;
  }
</script>

<Button on:click={handleClick}>{text} (Clicked {count} times)</Button>
