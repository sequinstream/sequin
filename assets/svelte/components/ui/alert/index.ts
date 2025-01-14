import { type VariantProps, tv } from "tailwind-variants";

import Description from "./alert-description.svelte";
import Title from "./alert-title.svelte";
import Root from "./alert.svelte";

export const alertVariants = tv({
  base: "[&>svg]:text-foreground relative w-full rounded-lg border p-4 [&:has(svg)]:pl-11 [&>svg+div]:translate-y-[-3px] [&>svg]:absolute [&>svg]:left-4 [&>svg]:top-4",

  variants: {
    variant: {
      default: "bg-background text-foreground",
      destructive:
        "border-red-500 text-red-600 dark:border-red-500 [&>svg]:text-red-600 bg-red-50",
      warning:
        "border-yellow-500 text-yellow-600 dark:border-yellow-500 [&>svg]:text-yellow-600 bg-yellow-50",
      info: "border-blue-500 text-blue-600 dark:border-blue-500 [&>svg]:text-blue-600 bg-blue-50",
    },
  },
  defaultVariants: {
    variant: "default",
  },
});

export type Variant = VariantProps<typeof alertVariants>["variant"];
export type HeadingLevel = "h1" | "h2" | "h3" | "h4" | "h5" | "h6";

export {
  //
  Root as Alert,
  Description as AlertDescription,
  Title as AlertTitle,
  Description,
  Root,
  Title,
};
