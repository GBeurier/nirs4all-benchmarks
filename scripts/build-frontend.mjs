import { build } from "esbuild";
import { fileURLToPath } from "node:url";
import { readFile, readdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
const root = fileURLToPath(new URL("../", import.meta.url));
await build({
  absWorkingDir: root,
  entryPoints: ["frontend/quality-ranking.tsx"],
  outfile: "src/nirs4all_benchmarks/web/generated/quality-ranking.js",
  bundle: true, format: "esm", target: ["es2022"], jsx: "automatic",
  minify: true, sourcemap: false, legalComments: "eof",
  loader: { ".css": "text" },
  define: { "process.env.NODE_ENV": '"production"' },
  logLevel: "info",
});

// Keep complete distribution notices alongside the generated runtime bundle.
const noticeFiles = [
  "react/LICENSE", "react-dom/LICENSE", "scheduler/LICENSE",
  "nirs4all-ui/LICENSE", "nirs4all-ui/LICENSING.md", "nirs4all-ui/THIRD_PARTY_NOTICES.md",
  ...(await readdir(join(root, "node_modules/nirs4all-ui/LICENSES"))).sort().map(name => `nirs4all-ui/LICENSES/${name}`),
];
const notices = await Promise.all(noticeFiles.map(async name =>
  `===== ${name} =====\n\n${await readFile(join(root, "node_modules", name), "utf8")}`));
await writeFile(join(root, "src/nirs4all_benchmarks/web/generated/THIRD_PARTY_NOTICES.txt"), notices.join("\n\n"));
