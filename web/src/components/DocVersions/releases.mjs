/** Release identity and navigation shared by the site and its release build. */
export const versionPattern = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;
export const docsHome = '/docs/guides/';
export const manifestPath = '/docs-versions.json';

export function releaseLine(version) {
  const [major, minor] = version.split('.');
  return major === '0' ? `0.${minor}.x` : `${major}.x`;
}

export function releaseStatus(version, latest, archivedLines) {
  if (version === latest) return 'Latest';
  if (version.startsWith('0.') || archivedLines.includes(releaseLine(version))) return 'Archived';
  return undefined;
}

export function archiveBase(version) {
  if (!versionPattern.test(version)) throw new Error(`Invalid Vigilo release: ${version}`);
  return `/releases/${version}`;
}

export function compareVersions(left, right) {
  const a = left.split('.').map(BigInt);
  const b = right.split('.').map(BigInt);
  for (let index = 0; index < a.length; index++) {
    if (a[index] !== b[index]) return a[index] > b[index] ? 1 : -1;
  }
  return 0;
}

export function docsPath(pathname, baseUrl) {
  const path = path => path.replace(/\/+$/, '');
  return path(baseUrl) && pathname.startsWith(baseUrl)
    ? '/' + path(pathname.slice(baseUrl.length))
    : path(pathname);
}

export function destination(base, path, routes, fragment = '') {
  const matched = routes.includes(path);
  return base + (matched ? path + '/' + fragment : docsHome);
}
