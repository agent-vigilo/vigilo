import {useEffect, useRef, useState, type MouseEvent, type ReactNode} from 'react';
import * as Popover from '@radix-ui/react-popover';
import Head from '@docusaurus/Head';
import {useLocation} from '@docusaurus/router';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

import {archiveBase, destination, docsHome, docsPath, manifestPath, releaseMetaName, releaseLine, releaseStatus, versionPattern} from './releases.mjs';
import styles from './styles.module.css';

type Release = {version: string};
type Manifest = {releases: Release[]; archivedLines: string[]};

let manifestRequest: Promise<Manifest> | undefined;

function loadManifest(): Promise<Manifest> {
  manifestRequest ??= fetch(manifestPath).then(async (response) => {
    if (!response.ok) throw new Error('Unable to load releases');
    const data: Manifest = await response.json();
    if (!Array.isArray(data.releases) || !data.releases.every((release) => typeof release?.version === 'string' && versionPattern.test(release.version)) ||
        !Array.isArray(data.archivedLines)) {
      throw new Error('Invalid documentation release manifest');
    }
    return data;
  }).catch((error: unknown) => {
    manifestRequest = undefined;
    throw error;
  });
  return manifestRequest;
}

function useManifest(enabled: boolean) {
  const [manifest, setManifest] = useState<Manifest>();
  const [failed, setFailed] = useState(false);
  useEffect(() => {
    if (!enabled) return;
    let active = true;
    setFailed(false);
    void loadManifest().then(
      (result) => { if (active) setManifest(result); },
      () => { if (active) setFailed(true); },
    );
    return () => { active = false; };
  }, [enabled]);
  return {manifest, failed};
}

/** Switch using the target release's route list, including preserved anchors. */
async function switchRelease(event: MouseEvent<HTMLAnchorElement>, base: string, page: string) {
  if (event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
  event.preventDefault();
  let target = base + docsHome;
  try {
    const response = await fetch(base + '/docs-routes.json', {signal: AbortSignal.timeout(3000)});
    if (response.ok) target = destination(base, page, await response.json(), window.location.hash);
  } catch {
    // The release landing page stays available if its route list cannot be loaded.
  }
  window.location.assign(target);
}

export function DocReleaseMetadata(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const release = siteConfig.customFields?.docsRelease as string | null;
  return release ? <Head><meta name={releaseMetaName} content={release} /></Head> : null;
}

export function DocVersionPicker(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const release = siteConfig.customFields?.docsRelease as string | null;
  const beta = siteConfig.customFields?.docsBeta === true;
  const label = release ? `v${release}` : 'Unreleased';
  const location = useLocation();
  const [open, setOpen] = useState(false);
  const heading = useRef<HTMLParagraphElement>(null);
  const {manifest, failed} = useManifest(open && Boolean(release));

  const page = docsPath(location.pathname, siteConfig.baseUrl);
  const latest = manifest?.releases[0]?.version;
  const lines = new Map<string, string>();
  if (manifest) {
    for (const {version: published} of manifest.releases) {
      if (releaseStatus(published, latest, manifest.archivedLines) === 'Archived') continue;
      const line = releaseLine(published);
      if (!lines.has(line)) lines.set(line, published);
    }
  }

  return (
    <Popover.Root open={open} onOpenChange={setOpen}>
      <Popover.Trigger className={styles.trigger} aria-label={`Documentation version: ${label}`}>
        <span>{label}</span>
        <svg viewBox="0 0 16 16" aria-hidden="true" width="14" height="14">
          <path d="m4 6 4 4 4-4" fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round"/>
        </svg>
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Content className={styles.panel} aria-label="Documentation versions"
          align="end" sideOffset={8} collisionPadding={12}
          onOpenAutoFocus={(event) => {
            event.preventDefault();
            heading.current?.focus();
          }}>
          <p ref={heading} tabIndex={-1} className={styles.heading}>Documentation versions</p>
          {!release && <p className={styles.feedback}>You’re reading the development docs.</p>}
          {release && manifest && releaseStatus(release, latest, manifest.archivedLines) === 'Archived' &&
            <p className={styles.feedback}>You’re reading an archived release.</p>}
          {beta &&
            <p className={styles.betaNotice}><strong>Pre-v1 beta.</strong> Expect instability and breaking changes.</p>}
          {failed && <p role="status" className={styles.feedback}>Release list is unavailable.</p>}
          {release && !manifest && !failed && <p role="status" className={styles.feedback}>Loading releases…</p>}
          {Array.from(lines, ([line, target]) => {
            const isLatest = latest === target;
            const base = isLatest ? '' : archiveBase(target);
            const current = target === release;
            const isBeta = target.startsWith('0.');
            return (
              <a key={line} className={styles.item} aria-current={current ? 'true' : undefined}
                href={base + docsHome} onClick={(event) => { void switchRelease(event, base, page); }}>
                <span className={styles.line}>{isBeta ? `v${target}` : line}</span>
                <span className={styles.version}>
                  {isLatest && 'Latest'}
                  {!isBeta && (isLatest ? ` · v${target}` : `v${target}`)}
                  {current && <span className={styles.check} aria-hidden="true">✓</span>}
                </span>
              </a>
            );
          })}
          <a className={styles.history} href="/docs/releases/">Release history <span aria-hidden="true">→</span></a>
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  );
}

export function ReleaseHistory(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const released = Boolean(siteConfig.customFields?.docsRelease);
  const {manifest, failed} = useManifest(released);
  if (!released || manifest?.releases.length === 0) {
    return <p>Documentation snapshots appear here as Vigilo releases are published.</p>;
  }
  if (failed) return <p role="status">Release history is unavailable.</p>;
  if (!manifest) return <p role="status">Loading releases…</p>;
  return (
    <ul className={styles.releases}>
      {manifest.releases.map(({version}) => {
        const label = releaseStatus(version, manifest.releases[0]?.version, manifest.archivedLines);
        return (
          <li key={version}>
            <a href={archiveBase(version) + docsHome}>v{version}</a>
            {label && <span className={styles.status}>{label}</span>}
          </li>
        );
      })}
    </ul>
  );
}
