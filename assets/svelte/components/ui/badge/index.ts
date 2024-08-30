import { type VariantProps, tv } from "tailwind-variants";
export { default as Badge } from "./badge.svelte";

export const badgeVariants = tv({
	base: "focus:ring-ring inline-flex select-none items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2 truncate",
	variants: {
		variant: {
			default: "bg-primary text-primary-foreground border-transparent",
			secondary:
				"bg-secondary text-secondary-foreground border-transparent",
			destructive:
				"bg-destructive text-destructive-foreground border-transparent",
			outline: "text-foreground",
			active: "bg-purple-500 text-white border-transparent",
			disabled: "bg-gray-400 text-white border-transparent",
			healthy: "bg-green-100 text-green-800 border-transparent",
			warning: "bg-yellow-100 text-yellow-800 border-transparent",
			error: "bg-red-100 text-red-800 border-transparent",
			initializing: "bg-blue-100 text-blue-800 border-transparent",
		},
	},
	defaultVariants: {
		variant: "default",
	},
});

export type Variant = VariantProps<typeof badgeVariants>["variant"];
