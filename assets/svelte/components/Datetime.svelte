<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import { Button } from "$lib/components/ui/button";
  import * as Popover from "$lib/components/ui/popover";
  import {
    Select,
    SelectTrigger,
    SelectValue,
    SelectContent,
    SelectItem,
  } from "$lib/components/ui/select";

  export let value: Date;
  value = value || new Date(new Date().getFullYear(), new Date().getMonth(), 1);
  export let error: string = "";
  export let onChange: (date: Date) => void;

  const months = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
  ];

  let day = value.getDate().toString().padStart(2, "0");
  let month = months[value.getMonth()];
  let year = value.getFullYear().toString();
  let hours = value.getHours().toString().padStart(2, "0");
  let minutes = value.getMinutes().toString().padStart(2, "0");
  let seconds = value.getSeconds().toString().padStart(2, "0");

  // Validate day
  $: {
    let dayNum = parseInt(day);
    if (!isNaN(dayNum) && dayNum > 31) {
      day = "31";
    }
  }

  // Validate year
  $: {
    let yearNum = parseInt(year);
    if (!isNaN(yearNum) && yearNum > 9999) {
      year = "9999";
    }
  }

  // Validate hours
  $: {
    let hoursNum = parseInt(hours);
    if (!isNaN(hoursNum) && hoursNum > 23) {
      hours = "23";
    }
  }

  // Validate minutes
  $: {
    let minutesNum = parseInt(minutes);
    if (!isNaN(minutesNum) && minutesNum > 59) {
      minutes = "59";
    }
  }

  // Validate seconds
  $: {
    let secondsNum = parseInt(seconds);
    if (!isNaN(secondsNum) && secondsNum > 59) {
      seconds = "59";
    }
  }

  // Initialize local variables from the initial value
  $: {
    const monthIndex = months.indexOf(month);
    const yearNum = parseInt(year);
    const dayNum = parseInt(day);

    if (!isValidDate(yearNum, monthIndex, dayNum)) {
      error = "Invalid date.";
    } else {
      error = "";
    }

    const newDate = new Date(
      Date.UTC(
        yearNum,
        monthIndex,
        dayNum,
        parseInt(hours),
        parseInt(minutes),
        parseInt(seconds),
      ),
    );
    // Only update if the new date is different to prevent infinite loops
    if (+newDate !== +value) {
      value = newDate;
      onChange(newDate);
    }
  }

  let iso8601Input = "";
  let iso8601Error = "";
  let isPopoverOpen = false;

  function autofillFromISO8601(event: Event) {
    const trimmedInput = iso8601Input.trim().replace(/^['"]|['"]$/g, "");
    const date = new Date(trimmedInput);

    if (isNaN(date.getTime())) {
      iso8601Error = "Invalid ISO8601 format";
    } else {
      iso8601Error = "";
      day = date.getUTCDate().toString().padStart(2, "0");
      month = months[date.getUTCMonth()];
      year = date.getUTCFullYear().toString();
      hours = date.getUTCHours().toString().padStart(2, "0");
      minutes = date.getUTCMinutes().toString().padStart(2, "0");
      seconds = date.getUTCSeconds().toString().padStart(2, "0");

      // Close the popover after successful autofill
      isPopoverOpen = false;
      iso8601Input = ""; // Clear the input field
    }
  }

  // Add this validation function
  function isValidDate(y: number, m: number, d: number): boolean {
    const date = new Date(y, m, d);
    return (
      date.getFullYear() === y && date.getMonth() === m && date.getDate() === d
    );
  }
</script>

<div class="space-y-2 {$$props.class}">
  <div class="flex space-x-6">
    <div class="space-y-2">
      <Label class="text-sm text-muted-foreground">Date</Label>
      <div class="flex space-x-2">
        <Input type="number" bind:value={day} class="w-16" min={1} max={31} />
        <Select
          selected={{ value: month, label: month }}
          onSelectedChange={(event) => {
            month = event.value;
          }}
        >
          <SelectTrigger class="w-[140px]">
            <SelectValue placeholder="Month" />
          </SelectTrigger>
          <SelectContent>
            {#each months as m}
              <SelectItem value={m}>{m}</SelectItem>
            {/each}
          </SelectContent>
        </Select>
        <Input
          type="number"
          bind:value={year}
          class="w-20"
          min={100}
          max={9999}
        />
      </div>
    </div>
    <div class="space-y-2 flex flex-col justify-between">
      <div class="grid grid-cols-[1fr_auto] gap-2 items-center">
        <Label class="text-sm text-muted-foreground">Time</Label>
        <Popover.Root bind:open={isPopoverOpen}>
          <Popover.Trigger asChild let:builder>
            <Button
              builders={[builder]}
              variant="link"
              class="text-xs text-blue-500 cursor-pointer p-0 h-auto"
            >
              Autofill from ISO8601
            </Button>
          </Popover.Trigger>
          <Popover.Content class="w-80">
            <div class="grid gap-4">
              <div class="space-y-2">
                <p class="text-sm text-muted-foreground">
                  Enter an ISO8601 formatted datetime string.
                </p>
              </div>
              <div class="flex space-x-2">
                <Input
                  type="text"
                  bind:value={iso8601Input}
                  placeholder="YYYY-MM-DDTHH:mm:ssZ"
                />
                <Button
                  variant="secondary"
                  on:click={autofillFromISO8601}
                  disabled={!iso8601Input}
                >
                  Autofill
                </Button>
              </div>
              {#if iso8601Error}
                <p class="text-sm text-red-500">{iso8601Error}</p>
              {/if}
            </div>
          </Popover.Content>
        </Popover.Root>
      </div>
      <div class="flex space-x-2">
        <Input
          type="number"
          bind:value={hours}
          class="w-16 text-center"
          min={0}
          max={23}
        />
        <Input
          type="number"
          bind:value={minutes}
          class="w-16 text-center"
          min={0}
          max={59}
        />
        <Input
          type="number"
          bind:value={seconds}
          class="w-16 text-center"
          min={0}
          max={59}
        />
        <span class="flex items-center text-sm text-muted-foreground">UTC</span>
      </div>
    </div>
  </div>
</div>
