<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import {
    Select,
    SelectTrigger,
    SelectValue,
    SelectContent,
    SelectItem,
  } from "$lib/components/ui/select";

  export let value: Date = new Date(
    new Date().getFullYear(),
    new Date().getMonth(),
    1
  );

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
    if (isNaN(dayNum) || dayNum < 1) {
      day = "01";
    } else if (dayNum > 31) {
      day = "31";
    } else {
      day = dayNum.toString().padStart(2, "0");
    }
  }

  // Validate year
  $: {
    let yearNum = parseInt(year);
    if (isNaN(yearNum) || yearNum < 1900) {
      year = "1900";
    } else if (yearNum > 9999) {
      year = "9999";
    } else {
      year = yearNum.toString().padStart(4, "0");
    }
  }

  // Validate hours
  $: {
    let hoursNum = parseInt(hours);
    if (isNaN(hoursNum) || hoursNum < 0) {
      hours = "00";
    } else if (hoursNum > 23) {
      hours = "23";
    } else {
      hours = hoursNum.toString().padStart(2, "0");
    }
  }

  // Validate minutes
  $: {
    let minutesNum = parseInt(minutes);
    if (isNaN(minutesNum) || minutesNum < 0) {
      minutes = "00";
    } else if (minutesNum > 59) {
      minutes = "59";
    } else {
      minutes = minutesNum.toString().padStart(2, "0");
    }
  }

  // Validate seconds
  $: {
    let secondsNum = parseInt(seconds);
    if (isNaN(secondsNum) || secondsNum < 0) {
      seconds = "00";
    } else if (secondsNum > 59) {
      seconds = "59";
    } else {
      seconds = secondsNum.toString().padStart(2, "0");
    }
  }

  // Initialize local variables from the initial value
  $: {
    const newDate = new Date(
      Date.UTC(
        parseInt(year),
        months.indexOf(month),
        parseInt(day),
        parseInt(hours),
        parseInt(minutes),
        parseInt(seconds)
      )
    );
    // Only update if the new date is different to prevent infinite loops
    if (+newDate !== +value) {
      value = newDate;
    }
  }
</script>

<div class="space-y-2">
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
          min={1900}
          max={9999}
        />
      </div>
    </div>
    <div class="space-y-2">
      <Label class="text-sm text-muted-foreground">Time</Label>
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
