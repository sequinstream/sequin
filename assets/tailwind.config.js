import { fontFamily } from "tailwindcss/defaultTheme";
import path from "path";
import fs from "fs";

/** @type {import('tailwindcss').Config} */
const config = {
  darkMode: ["class"],
  content: [
    "./src/**/*.{html,js,svelte,ts}",
    "./svelte/**/*.{svelte,js,ts}",
    "./js/**/*.js",
    "../lib/sequin_web.ex",
    "../lib/sequin_web/**/*.*ex",
  ],
  safelist: ["dark"],
  theme: {
    container: {
      center: true,
      padding: "2rem",
      screens: {
        "2xl": "1400px",
      },
    },
    extend: {
      colors: {
        border: "hsl(var(--border) / <alpha-value>)",
        input: "hsl(var(--input) / <alpha-value>)",
        ring: "hsl(var(--ring) / <alpha-value>)",
        background: "hsl(var(--background) / <alpha-value>)",
        foreground: "hsl(var(--foreground) / <alpha-value>)",
        primary: {
          DEFAULT: "hsl(var(--primary) / <alpha-value>)",
          foreground: "hsl(var(--primary-foreground) / <alpha-value>)",
        },
        secondary: {
          DEFAULT: "hsl(var(--secondary) / <alpha-value>)",
          foreground: "hsl(var(--secondary-foreground) / <alpha-value>)",
          "3xSubtle": "rgb(var(--color-secondary-3xSubtle))",
          "2xSubtle": "rgb(var(--color-secondary-2xSubtle))",
        },
        destructive: {
          DEFAULT: "hsl(var(--destructive) / <alpha-value>)",
          foreground: "hsl(var(--destructive-foreground) / <alpha-value>)",
        },
        muted: {
          DEFAULT: "hsl(var(--muted) / <alpha-value>)",
          foreground: "hsl(var(--muted-foreground) / <alpha-value>)",
        },
        accent: {
          DEFAULT: "hsl(var(--accent) / <alpha-value>)",
          foreground: "hsl(var(--accent-foreground) / <alpha-value>)",
        },
        popover: {
          DEFAULT: "hsl(var(--popover) / <alpha-value>)",
          foreground: "hsl(var(--popover-foreground) / <alpha-value>)",
        },
        card: {
          DEFAULT: "hsl(var(--card) / <alpha-value>)",
          foreground: "hsl(var(--card-foreground) / <alpha-value>)",
        },
        brand: "#FD4F00",
        canvasBase: "rgb(var(--color-canvasBase))",
        canvasSubtle: "rgb(var(--color-canvasSubtle))",
        canvasMuted: "rgb(var(--color-canvasMuted))",
        subtle: "rgb(var(--color-subtle))",
        disabled: "rgb(var(--color-disabled))",
        info: {
          DEFAULT: "rgb(var(--color-foreground-info))",
        },
        basis: "rgb(var(--color-basis))",
        carbon: {
          0: "rgb(var(--color-carbon-0))",
          50: "rgb(var(--color-carbon-50))",
          100: "rgb(var(--color-carbon-100))",
          200: "rgb(var(--color-carbon-200))",
          300: "rgb(var(--color-carbon-300))",
          400: "rgb(var(--color-carbon-400))",
          500: "rgb(var(--color-carbon-500))",
          600: "rgb(var(--color-carbon-600))",
          700: "rgb(var(--color-carbon-700))",
          800: "rgb(var(--color-carbon-800))",
          900: "rgb(var(--color-carbon-900))",
          1000: "rgb(var(--color-carbon-1000))",
        },
        matcha: {
          0: "rgb(var(--color-matcha-0))",
          100: "rgb(var(--color-matcha-100))",
          200: "rgb(var(--color-matcha-200))",
          300: "rgb(var(--color-matcha-300))",
          400: "rgb(var(--color-matcha-400))",
          500: "rgb(var(--color-matcha-500))",
          600: "rgb(var(--color-matcha-600))",
          700: "rgb(var(--color-matcha-700))",
          800: "rgb(var(--color-matcha-800))",
          900: "rgb(var(--color-matcha-900))",
        },
        breeze: {
          0: "rgb(var(--color-breeze-0))",
          100: "rgb(var(--color-breeze-100))",
          1000: "rgb(var(--color-breeze-1000))",
        },
        ruby: {
          0: "rgb(var(--color-ruby-0))",
          100: "rgb(var(--color-ruby-100))",
          900: "rgb(var(--color-ruby-900))",
        },
        honey: {
          0: "rgb(var(--color-honey-0))",
          100: "rgb(var(--color-honey-100))",
          900: "rgb(var(--color-honey-900))",
        },
      },
      backgroundColor: {
        canvas: {
          base: "rgb(var(--color-background-canvas-base))",
          subtle: "rgb(var(--color-background-canvas-subtle))",
          muted: "rgb(var(--color-background-canvas-muted))",
        },
        surface: {
          base: "rgb(var(--color-background-surface-base))",
          subtle: "rgb(var(--color-background-surface-subtle))",
          muted: "rgb(var(--color-background-surface-muted))",
        },
      },
      textColor: {
        base: "rgb(var(--color-foreground-base))",
        subtle: "rgb(var(--color-foreground-subtle))",
        muted: "rgb(var(--color-foreground-muted))",
      },
      borderRadius: {
        lg: "var(--radius)",
        md: "calc(var(--radius) - 2px)",
        sm: "calc(var(--radius) - 4px)",
      },
      fontFamily: {
        sans: [...fontFamily.sans],
      },
    },
  },
  plugins: [
    require("@tailwindcss/forms"),
    require("@tailwindcss/typography"),
    ({ addVariant }) => {
      addVariant("phx-no-feedback", [
        ".phx-no-feedback&",
        ".phx-no-feedback &",
      ]);
      addVariant("phx-click-loading", [
        ".phx-click-loading&",
        ".phx-click-loading &",
      ]);
      addVariant("phx-submit-loading", [
        ".phx-submit-loading&",
        ".phx-submit-loading &",
      ]);
      addVariant("phx-change-loading", [
        ".phx-change-loading&",
        ".phx-change-loading &",
      ]);
    },
    // Embeds heroicons, bundled with phoenix
    function ({ matchComponents, theme }) {
      let iconsDir = path.join(__dirname, "./vendor/heroicons/optimized");
      let values = {};
      let icons = [
        ["", "/24/outline"],
        ["-solid", "/24/solid"],
        ["-mini", "/20/solid"],
        // ["-micro", "/16/solid"],
      ];
      icons.forEach(([suffix, dir]) => {
        fs.readdirSync(path.join(iconsDir, dir)).forEach((file) => {
          let name = path.basename(file, ".svg") + suffix;
          values[name] = { name, fullPath: path.join(iconsDir, dir, file) };
        });
      });
      matchComponents(
        {
          hero: ({ name, fullPath }) => {
            let content = fs
              .readFileSync(fullPath)
              .toString()
              .replace(/\r?\n|\r/g, "");
            let size = theme("spacing.6");
            if (name.endsWith("-mini")) {
              size = theme("spacing.5");
            } else if (name.endsWith("-micro")) {
              size = theme("spacing.4");
            }
            return {
              [`--hero-${name}`]: `url('data:image/svg+xml;utf8,${content}')`,
              "-webkit-mask": `var(--hero-${name})`,
              mask: `var(--hero-${name})`,
              "mask-repeat": "no-repeat",
              "background-color": "currentColor",
              "vertical-align": "middle",
              display: "inline-block",
              width: size,
              height: size,
            };
          },
        },
        { values }
      );
    },
  ],
};

export default config;
