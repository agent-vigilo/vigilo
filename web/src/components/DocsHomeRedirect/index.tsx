import type {ReactNode} from 'react';
import {useEffect} from 'react';
import Link from '@docusaurus/Link';
import Head from '@docusaurus/Head';
import Layout from '@theme/Layout';
import useBaseUrl from '@docusaurus/useBaseUrl';
import {docsHome} from '@site/src/components/DocVersions/releases.mjs';

export default function DocsHomeRedirect(): ReactNode {
  const target = useBaseUrl(docsHome);
  const canonical = useBaseUrl(docsHome, {absolute: true});
  useEffect(() => {
    window.location.replace(target);
  }, [target]);

  return (
    <Layout
      title="Guides"
      description="Agent Vigilo guides for getting started, creating evaluators, and publishing evaluator artifacts.">
      <Head>
        <link rel="canonical" href={canonical} />
        <meta name="robots" content="noindex,follow" />
        <meta httpEquiv="refresh" content={`0; url=${target}`} />
      </Head>
      <main className="container margin-vert--xl">
        <div className="row">
          <div className="col col--8 col--offset-2">
            <h1>Guides</h1>
            <p>Find getting started, configuration, and evaluator guides.</p>
            <Link className="button button--primary" to={docsHome}>
              Open Guides
            </Link>
          </div>
        </div>
      </main>
    </Layout>
  );
}
