import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { build } from 'esbuild';
import { finishHostFactory } from '../scripts/host-factory-imports.mjs';
import { loadHostModule } from '../src/host-component.mjs';
import metadata from '../package.json' with { type: 'json' };

test('host factories preserve lazy imports, host identity and rejected-import retries', async t => {
  const root = await mkdtemp(join(tmpdir(), 'omd-lazy-host-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const file = join(root, 'factory.mjs');
  const result = await build({ stdin: { contents: `
    import { identity } from 'host-static';
    export { identity };
    export const text = 'import("literal text")';
    export const native = () => import('koffi');
    export const same = () => import('host-static');
    export const builtin = () => import('node:path');
  `, resolveDir: root }, outfile: file, bundle: true, format: 'cjs', platform: 'node', target: 'node22', packages: 'external', metafile: true,
    banner: { js: 'export function createModule(require, importHostModule) { const module = { exports: {} }; const exports = module.exports;' },
    footer: { js: 'return module.exports; }' } });
  const dependencies = await finishHostFactory(result, file);
  assert.deepEqual(dependencies, ['host-static']);
  const { createModule } = await import(pathToFileURL(file));
  const calls = [], identity = Symbol('host'), native = { default: { native: true } };
  const failure = new Error('native load failed'); let fail = true;
  const tree = { ctx: { loader: { internal: { import: async () => ({ default: { version: metadata.engines.dsh } }) } } }, import(name) {
    calls.push(name);
    if (name === 'host-static') return { identity };
    assert.equal(name, 'koffi');
    if (fail) { fail = false; throw failure; }
    return native;
  } };
  const ctx = { loader: { entries: () => [{ options: { name: '@deepseek-ai/dsh-tool-bash' }, parent: { tree } }] } };
  const component = await loadHostModule(ctx, 'tool-bash', createModule, dependencies);
  assert.deepEqual(calls, ['host-static']);
  assert.equal(component.identity, identity);
  assert.equal(component.text, 'import("literal text")');
  await assert.rejects(component.native(), error => error === failure);
  assert.equal(await component.native(), native);
  assert.equal((await component.same()).identity, identity);
  assert.equal((await component.builtin()).join('a', 'b'), join('a', 'b'));
});

test('real workflow factories keep Windows bindings out of eager imports', async () => {
  const dependencies = JSON.parse(await readFile(new URL('../lib/host/dependencies.json', import.meta.url), 'utf8'));
  for (const list of Object.values(dependencies)) assert(!list.includes('koffi'));
  const factory = await readFile(new URL('../lib/host/workflow-ptc.factory.mjs', import.meta.url), 'utf8');
  assert(factory.includes('await importHostModule("koffi")'));
});
