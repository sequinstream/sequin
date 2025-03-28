import { type ClassValue, clsx } from "clsx";
import { formatDistanceToNow } from "date-fns";
import { cubicOut } from "svelte/easing";
import type { TransitionConfig } from "svelte/transition";
import { twMerge } from "tailwind-merge";
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatRelativeTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  const now = new Date();
  const diffInMilliseconds = now.getTime() - date.getTime();
  const diffInDays = Math.floor(diffInMilliseconds / (1000 * 60 * 60 * 24));
  const diffInWeeks = Math.floor(diffInDays / 7);
  const diffInMonths =
    (now.getFullYear() - date.getFullYear()) * 12 +
    now.getMonth() -
    date.getMonth();
  const diffInYears = now.getFullYear() - date.getFullYear();

  if (diffInDays < 7) {
    return formatDistanceToNow(date, { addSuffix: true });
  } else if (diffInWeeks === 1) {
    return "last week";
  } else if (diffInWeeks < 4) {
    return `${diffInWeeks} weeks ago`;
  } else if (diffInMonths === 1) {
    return "last month";
  } else if (diffInMonths < 12) {
    return `${diffInMonths} months ago`;
  } else if (diffInYears === 1) {
    return "last year";
  } else {
    return `${diffInYears} years ago`;
  }
}

export function truncateMiddle(text: string, maxLength: number): string {
  if (text.length <= maxLength) return text;
  const startChars = Math.ceil((maxLength - 3) / 2);
  const endChars = Math.floor((maxLength - 3) / 2);
  return (
    text.substring(0, startChars) +
    "..." +
    text.substring(text.length - endChars)
  );
}

export function getColorFromName(name: string): string {
  const colors = [
    "bg-red-100",
    "bg-red-300",
    "bg-blue-100",
    "bg-blue-300",
    "bg-green-100",
    "bg-green-300",
    "bg-indigo-100",
    "bg-indigo-300",
    "bg-purple-100",
    "bg-purple-300",
    "bg-pink-100",
    "bg-pink-300",
    "bg-teal-100",
    "bg-teal-300",
  ];
  const hash = name.split("").reduce((acc, char) => {
    return char.charCodeAt(0) + ((acc << 5) - acc);
  }, 0);
  const index = Math.abs(hash) % colors.length;
  return colors[index];
}

type FlyAndScaleParams = {
  y?: number;
  x?: number;
  start?: number;
  duration?: number;
};

export const flyAndScale = (
  node: Element,
  params: FlyAndScaleParams = { y: -8, x: 0, start: 0.95, duration: 150 },
): TransitionConfig => {
  const style = getComputedStyle(node);
  const transform = style.transform === "none" ? "" : style.transform;

  const scaleConversion = (
    valueA: number,
    scaleA: [number, number],
    scaleB: [number, number],
  ) => {
    const [minA, maxA] = scaleA;
    const [minB, maxB] = scaleB;

    const percentage = (valueA - minA) / (maxA - minA);
    const valueB = percentage * (maxB - minB) + minB;

    return valueB;
  };

  const styleToString = (
    style: Record<string, number | string | undefined>,
  ): string => {
    return Object.keys(style).reduce((str, key) => {
      if (style[key] === undefined) return str;
      return str + `${key}:${style[key]};`;
    }, "");
  };

  return {
    duration: params.duration ?? 200,
    delay: 0,
    css: (t) => {
      const y = scaleConversion(t, [0, 1], [params.y ?? 5, 0]);
      const x = scaleConversion(t, [0, 1], [params.x ?? 0, 0]);
      const scale = scaleConversion(t, [0, 1], [params.start ?? 0.95, 1]);

      return styleToString({
        transform: `${transform} translate3d(${x}px, ${y}px, 0) scale(${scale})`,
        opacity: t,
      });
    },
    easing: cubicOut,
  };
};

export function formatNumberWithCommas(number: number | null): string {
  if (number === null) {
    return "N/A";
  }
  return number.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

/**
 * Conditionally applies a style class based on a condition
 * @param condition Boolean condition to evaluate
 * @param trueClass Class to apply if condition is true
 * @param falseClass Optional class to apply if condition is false
 * @returns The class to apply based on the condition
 */
export function conditionalClass(
  condition: boolean,
  trueClass: string,
  falseClass: string = "",
): string {
  return condition ? trueClass : falseClass;
}
