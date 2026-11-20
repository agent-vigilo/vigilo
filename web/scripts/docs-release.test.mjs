import assert from 'node:assert/strict';
import {mkdtemp, mkdir, readFile, rm, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {test} from 'node:test';

import {identity, mergeManifest, writeRoutes} from './docs-release.mjs';
import {archiveBase, destination, docsPath, releaseLine, releaseStatus} from '../src/components/DocVersions/releases.mjs';

const release = (version, digit = 'a') => identity(version, `vigilo/v${version}`, digit.repeat(40));
const empty = {schemaVersion: 1, releases: [], archivedLines: []};

test('an older-line patch is retained without replacing the latest docs', () => {
  const initial = mergeManifest(empty, release('2.10.0'), []);
  assert.equal(initial.refreshLatest, true);
  const backfill = mergeManifest(initial.manifest, release('1.9.0'), []);
  const patch = mergeManifest(backfill.manifest, release('1.9.1'), []);
  assert.equal(patch.latest, '2.10.0');
  assert.equal(patch.refreshLatest, false);
  assert.deepEqual(patch.manifest.releases.map(row => row.version), ['2.10.0', '1.9.1', '1.9.0']);
  assert.deepEqual(mergeManifest(patch.manifest, release('1.9.1'), []), patch);
  assert.equal(mergeManifest(patch.manifest, release('2.10.0'), []).refreshLatest, false);
});

test('archive policy preserves exact snapshots and rejects hiding the latest line', () => {
  const releases = [release('2.10.0'), release('0.2.0'), release('0.1.0')];
  const state = {schemaVersion: 1, releases, archivedLines: []};
  assert.deepEqual(mergeManifest(state, undefined, ['0.1.x']).manifest,
    {schemaVersion: 1, releases, archivedLines: ['0.1.x']});
  assert.equal(releaseLine('0.2.5'), '0.2.x');
  assert.equal(releaseLine('1.2.5'), '1.x');
  assert.throws(() => mergeManifest(state, undefined, ['2.x']), /latest release line/);
});

test('only the latest beta is selectable and stable archive policy stays independent', () => {
  assert.equal(releaseStatus('0.2.1', '0.2.1', []), 'Latest');
  assert.equal(releaseStatus('0.2.0', '0.2.1', []), 'Archived');
  assert.equal(releaseStatus('0.1.9', '0.2.1', []), 'Archived');
  assert.equal(releaseStatus('0.2.1', '1.0.0', []), 'Archived');
  assert.equal(releaseStatus('1.9.1', '2.0.0', []), undefined);
  assert.equal(releaseStatus('1.9.1', '2.0.0', ['1.x']), 'Archived');
});

test('release metadata rejects malformed identities, moved tags and invalid manifests', () => {
  const current = mergeManifest(empty, release('0.1.0'), []).manifest;
  assert.throws(() => identity('0.1.0', 'evaluators/v0.1.0', 'a'.repeat(40)), /identity/);
  assert.throws(() => identity('0.1.0/..', 'vigilo/v0.1.0/..', 'a'.repeat(40)), /identity/);
  assert.throws(() => mergeManifest(current, release('0.1.0', 'b'), []), /tag moved/);
  assert.throws(() => mergeManifest({...current, schemaVersion: 2}, undefined, []), /manifest/);
  assert.throws(() => mergeManifest(current, undefined, ['../archive']), /policy/);
});

test('switching preserves an available page and falls back inside the selected release', () => {
  const base = archiveBase('0.1.0');
  assert.equal(docsPath('/releases/0.1.0/docs/a/', base + '/'), '/docs/a');
  assert.equal(docsPath('/docs/a/', '/'), '/docs/a');
  assert.equal(destination(base, '/docs/a', ['/docs/a'], '#anchor'), base + '/docs/a/#anchor');
  assert.equal(destination(base, '/docs/missing', ['/docs/a'], '#anchor'), base + '/docs/guides/');
});

test('archive route validation catches a compiled link escaping the snapshot', async () => {
  const root = await mkdtemp(path.join(tmpdir(), 'vigilo-docs-routes-'));
  try {
    await mkdir(path.join(root, 'docs/guides'), {recursive: true});
    await mkdir(path.join(root, 'docs/releases'));
    const guides = path.join(root, 'docs/guides/index.html');
    await writeFile(path.join(root, 'docs/releases/index.html'), '<h1>Releases</h1>');
    await writeFile(guides, '<a href="/docs/guides/">Guide</a>');
    await assert.rejects(writeRoutes(root, '0.1.0'), /escapes/);
    await writeFile(guides, '<a href=/examples/cases.yaml>Example</a>');
    await assert.rejects(writeRoutes(root, '0.1.0'), /escapes/);
    await writeFile(guides, '<a href="/releases/0.1.0/docs/guides/#start">Guide</a>');
    assert.equal(await writeRoutes(root, '0.1.0'), 2);
    assert.deepEqual(JSON.parse(await readFile(path.join(root, 'docs-routes.json'), 'utf8')), [
      '/docs/guides', '/docs/releases',
    ]);
  } finally {
    await rm(root, {recursive: true, force: true});
  }
});
