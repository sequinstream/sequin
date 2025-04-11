This directory contains .mdx files that are shared between docs and svelte components (or other places, as needed).

Right now we do this for <button>Copy for ChatGPT</button> buttons, so we can pull in /docs reference snippets in the svelte components.

Symlinks are used in `docs/snippets` to pull in the shared mdx files for Mintlify and in `assets/svelte/mdx` to pull in the shared mdx files for Svelte.