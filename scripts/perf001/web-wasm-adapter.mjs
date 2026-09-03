#!/home/delete/.nvm/versions/node/v22.21.1/bin/node
/** PERF-001 stdio adapter for Web's shipped Core/Methods WASM closure. */

import { createHash } from 'node:crypto'
import { execFileSync } from 'node:child_process'
import { readFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const PROTOCOL = 'nirs4all.performance-compare.adapter.v1'
const WEB_COMMIT = '051bf636d7c1729087e5d40061b18bd690cd33b7'
const WEB_TREE = 'e94251e350f31dbb996e1a2e477c466cfdf992ff'

let input = ''
for await (const chunk of process.stdin) input += chunk
const request = JSON.parse(input)
if (request.protocol !== PROTOCOL || request.surface !== 'web_wasm') {
  throw new Error('unexpected PERF-001 adapter request')
}
if (request.candidate.commit_sha !== WEB_COMMIT) throw new Error('Web candidate mismatch')

const started = performance.now()
const here = dirname(fileURLToPath(import.meta.url))
const repository = resolve(here, '../..')
const workspace = resolve(repository, '../..')
const webRepository = resolve(workspace, '_worktrees/WEB-0.1.10-security')
const webRoot = resolve(webRepository, 'web-app')
const identity = execFileSync('/usr/bin/git', [
  '-C', webRepository, 'rev-parse', 'HEAD', 'HEAD^{tree}',
], { encoding: 'utf8' }).trim().split(/\r?\n/)
if (identity[0] !== WEB_COMMIT || identity[1] !== WEB_TREE) {
  throw new Error(`Web source identity mismatch: ${identity.join('/')}`)
}
const archive = await readFile(request.archive_v2.path)
const digest = createHash('sha256').update(archive).digest('hex')
if (digest !== request.archive_v2.sha256) throw new Error('Archive V2 digest mismatch')
const { replayMethodsArchiveV2 } = await import(
  pathToFileURL(resolve(webRoot, 'vendor/nirs4all/src/archive-v2.js')).href
)
const dataset = {
  X: request.matrix.x,
  rows: request.matrix.x.length,
  cols: request.matrix.x[0].length,
  sampleIds: request.matrix.sample_ids,
}

const predict = async () => {
  const result = await replayMethodsArchiveV2(archive, dataset)
  if (result.engine !== 'nirs4all-methods-wasm' || result.fallback !== false) {
    throw new Error('Web selected a non-WASM or fallback path')
  }
  if (result.nativePredictorDescriptor?.descriptor_fingerprint !== request.predictor_fingerprint) {
    throw new Error('Web native descriptor fingerprint mismatch')
  }
  const values = []
  for (let row = 0; row < result.rows; row += 1) {
    values.push(Array.from(result.data.slice(row * result.cols, (row + 1) * result.cols)))
  }
  return { result, values }
}

let current = await predict()
const startupMs = performance.now() - started
const steady = []
for (let repeat = 0; repeat < request.repeats; repeat += 1) {
  const before = performance.now()
  current = await predict()
  steady.push(performance.now() - before)
}

process.stdout.write(JSON.stringify({
  protocol: PROTOCOL,
  surface: 'web_wasm',
  commit_sha: WEB_COMMIT,
  archive_sha256: request.archive_v2.sha256,
  matrix_sha256: request.matrix_sha256,
  sample_ids: request.matrix.sample_ids,
  target_names: request.matrix.target_names,
  predictor_descriptor: request.predictor_descriptor,
  predictor_fingerprint: request.predictor_fingerprint,
  fallback_used: false,
  predictions: current.values,
  startup_ms: startupMs,
  steady_state_ms: steady,
  evidence: {
    kind: 'local_synthetic_current_head',
    entrypoint: 'replayMethodsArchiveV2',
    engine: current.result.engine,
    core_wasm: true,
    methods_wasm: true,
  },
}))
