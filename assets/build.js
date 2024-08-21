const esbuild = require("esbuild");
const sveltePlugin = require("esbuild-svelte");
const importGlobPlugin = require("esbuild-plugin-import-glob").default;
const sveltePreprocess = require("svelte-preprocess");
const postcss = require("postcss");
const tailwindcss = require("tailwindcss");
const autoprefixer = require("autoprefixer");

const args = process.argv.slice(2);
const watch = args.includes("--watch");
const deploy = args.includes("--deploy");

let optsClient = {
  entryPoints: ["js/app.js"],
  bundle: true,
  minify: deploy,
  target: "es2017",
  conditions: ["svelte", "browser"],
  outdir: "../priv/static/assets",
  logLevel: "info",
  sourcemap: watch ? "inline" : false,
  watch,
  tsconfig: "./tsconfig.json",
  plugins: [
    importGlobPlugin(),
    sveltePlugin({
      preprocess: sveltePreprocess({
        postcss: {
          plugins: [tailwindcss(), autoprefixer()],
        },
      }),
      compilerOptions: { dev: !deploy, hydratable: true, css: "injected" },
    }),
    {
      name: "css",
      setup(build) {
        build.onLoad({ filter: /\.css$/ }, async (args) => {
          const css = await require("fs").promises.readFile(args.path, "utf8");
          const result = await postcss([tailwindcss(), autoprefixer()]).process(
            css,
            { from: args.path }
          );
          return { contents: result.css, loader: "css" };
        });
      },
    },
  ],
};

let optsServer = {
  entryPoints: ["js/server.js"],
  platform: "node",
  bundle: true,
  minify: false,
  target: "node19.6.1",
  conditions: ["svelte"],
  outdir: "../priv/svelte",
  logLevel: "info",
  sourcemap: watch ? "inline" : false,
  watch,
  tsconfig: "./tsconfig.json",
  plugins: [
    importGlobPlugin(),
    sveltePlugin({
      preprocess: sveltePreprocess({
        postcss: {
          plugins: [tailwindcss(), autoprefixer()],
        },
      }),
      compilerOptions: { dev: !deploy, hydratable: true, generate: "ssr" },
    }),
  ],
};

const client = esbuild.build(optsClient);
const server = esbuild.build(optsServer);

if (watch) {
  client.then((_result) => {
    process.stdin.on("close", () => process.exit(0));
    process.stdin.resume();
  });

  server.then((_result) => {
    process.stdin.on("close", () => process.exit(0));
    process.stdin.resume();
  });
}
