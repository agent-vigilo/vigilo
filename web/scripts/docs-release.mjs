// Build release metadata and check the generated routes before publishing the site.
import {readFile, readdir, writeFile} from 'node:fs/promises';
import path from 'node:path';
import {pathToFileURL} from 'node:url';

import {archiveBase, compareVersions, docsHome, releaseLine, versionPattern} from '../src/components/DocVersions/releases.mjs';

const linePattern = /^(?:0\.(0|[1-9]\d*)|[1-9]\d*)\.x$/;
const shaPattern = /^[0-9a-f]{40}$/;

export function identity(version, tag, commit) {
  if (!versionPattern.test(version) || tag !== `vigilo/v${version}` || !shaPattern.test(commit)) {
    throw new Error('Invalid Vigilo release identity');
  }
  return {version, tag, commit};
}

export function mergeManifest(current, release, archivedLines) {
  if (current.schemaVersion !== 1 || !Array.isArray(current.releases)
      || !Array.isArray(archivedLines) || !archivedLines.every(line => typeof line === 'string' && linePattern.test(line))
      || new Set(archivedLines).size !== archivedLines.length) {
    throw new Error('Invalid documentation manifest or archive policy');
  }
  const releases = new Map();
  for (const entry of current.releases) {
    const item = identity(entry.version, entry.tag, entry.commit);
    if (releases.has(item.version)) throw new Error(`Duplicate release: ${item.version}`);
    releases.set(item.version, item);
  }
  const previous = [...releases.keys()].sort((a, b) => compareVersions(b, a))[0];
  if (release) {
    const item = identity(release.version, release.tag, release.commit);
    const known = releases.get(item.version);
    if (known && known.commit !== item.commit) throw new Error(`Release tag moved: ${item.tag}`);
    releases.set(item.version, item);
  }

  const ordered = [...releases.values()].sort((a, b) => compareVersions(b.version, a.version));
  if (ordered.length && archivedLines.includes(releaseLine(ordered[0].version))) {
    throw new Error('The latest release line must stay in the picker');
  }
  return {
    manifest: {schemaVersion: 1, releases: ordered, archivedLines},
    latest: ordered[0]?.version ?? '',
    refreshLatest: Boolean(release && (!previous || compareVersions(release.version, previous) > 0)),
  };
}

async function docsFiles(root, prefix) {
  const directory = path.join(root, prefix);
  const entries = await readdir(directory, {withFileTypes: true});
  const nested = await Promise.all(entries.map(async entry => {
    if (entry.isDirectory()) return docsFiles(root, prefix + '/' + entry.name);
    if (entry.name === 'index.html') return [{route: '/' + prefix, file: path.join(directory, entry.name)}];
    return [];
  }));
  return nested.flat();
}

export async function writeRoutes(root, archiveVersion) {
  const pages = await docsFiles(root, 'docs');
  const base = archiveVersion ? archiveBase(archiveVersion) : '';
  const routes = pages.map(page => page.route).sort();
  if (!routes.includes(docsHome.replace(/\/$/, ''))) throw new Error('Missing release docs home');

  // Check the rendered links. MDX and custom components can emit absolute paths
  // that work in a root build and escape an archived release.
  const docsRoutes = new Set(routes);
  const errors = [];
  if (archiveVersion) {
    for (const {route, file} of pages) {
      const html = await readFile(file, 'utf8');
      for (const [, quoted, singleQuoted, unquoted] of html.matchAll(/\s(?:href|src)=(?:"([^"]*)"|'([^']*)'|([^\s>]+))/g)) {
        const url = quoted ?? singleQuoted ?? unquoted;
        if (/^\/(?:docs|examples|assets|img)(?:[/?#]|$)/.test(url) && url !== '/docs/releases/') {
          errors.push(`${route} escapes to ${url}`);
        }
        if (!url.startsWith(base + '/docs/')) continue;
        const target = url.slice(base.length).split(/[?#]/, 1)[0].replace(/\/$/, '');
        if (!docsRoutes.has(target) && !path.extname(target)) errors.push(`${route} links to missing ${target}`);
      }
    }
  }
  if (errors.length) throw new Error([...new Set(errors)].join('\n'));

  await writeFile(path.join(root, 'docs-routes.json'), JSON.stringify(routes) + '\n');
  return routes.length;
}

async function readJson(file) {
  return JSON.parse(await readFile(file, 'utf8'));
}

async function main([command, ...args]) {
  if (command === 'routes') {
    const [root = 'build'] = args;
    const archive = process.env.VIGILO_DOCS_ARCHIVE === '1' ? process.env.VIGILO_DOCS_RELEASE : undefined;
    const count = await writeRoutes(root, archive);
    if (archive) {
      const search = await readJson(path.join(root, 'search-index.json'));
      const documents = search.flatMap(group => group.documents);
      if (documents.length === 0 || documents.some(doc => !doc.u.startsWith(archiveBase(archive) + '/docs/'))) {
        throw new Error('Archived search results must stay within their documentation snapshot');
      }
    }
    console.log(`Documentation routes validated: ${count}`);
  } else if (command === 'plan') {
    const [manifest, policy, output, version, tag, commit] = args;
    const current = await readJson(manifest);
    const archivedLines = policy === '-' ? current.archivedLines : (await readJson(policy)).archivedLines;
    const result = mergeManifest(current, version ? identity(version, tag, commit) : undefined, archivedLines);
    await writeFile(output, JSON.stringify(result.manifest, null, 2) + '\n');
    console.log(`latest=${result.latest}\nrefresh_latest=${result.refreshLatest}`);
  } else if (command === 'snapshot') {
    const [output, version, tag, commit] = args;
    await writeFile(output, JSON.stringify(identity(version, tag, commit)) + '\n');
  } else if (command === 'verify-snapshot') {
    const [file, version, tag, commit] = args;
    const snapshot = await readJson(file);
    const wanted = identity(version, tag, commit);
    if (snapshot.version !== wanted.version || snapshot.tag !== wanted.tag || snapshot.commit !== wanted.commit) {
      throw new Error(`Snapshot identity mismatch: ${tag}`);
    }
  } else {
    throw new Error(`Unknown docs release command: ${command}`);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await main(process.argv.slice(2));
}
