import { fontFamily } from "tailwindcss/defaultTheme";

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
  ],
};

// Embeds Heroicons (https://heroicons.com) into your app.css bundle
// See your `CoreComponents.icon/1` for more information.
//
// plugin(function ({ matchComponents, theme }) {
//   let iconsDir = path.join(__dirname, "../deps/heroicons/optimized");
//   let values = {};
//   let icons = [
//     ["", "/24/outline"],
//     ["-solid", "/24/solid"],
//     ["-mini", "/20/solid"],
//     ["-micro", "/16/solid"],
//   ];
//   icons.forEach(([suffix, dir]) => {
//     fs.readdirSync(path.join(iconsDir, dir)).forEach((file) => {
//       let name = path.basename(file, ".svg") + suffix;
//       values[name] = { name, fullPath: path.join(iconsDir, dir, file) };
//     });
//   });
//   matchComponents(
//     {
//       hero: ({ name, fullPath }) => {
//         let content = fs
//           .readFileSync(fullPath)
//           .toString()
//           .replace(/\r?\n|\r/g, "");
//         let size = theme("spacing.6");
//         if (name.endsWith("-mini")) {
//           size = theme("spacing.5");
//         } else if (name.endsWith("-micro")) {
//           size = theme("spacing.4");
//         }
//         return {
//           [`--hero-${name}`]: `url('data:image/svg+xml;utf8,${content}')`,
//           "-webkit-mask": `var(--hero-${name})`,
//           mask: `var(--hero-${name})`,
//           "mask-repeat": "no-repeat",
//           "background-color": "currentColor",
//           "vertical-align": "middle",
//           display: "inline-block",
//           width: size,
//           height: size,
//         };
//       },
//     },
//     { values }
//   );
// }),

export default config;
