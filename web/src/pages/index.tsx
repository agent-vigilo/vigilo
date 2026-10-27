import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Head from '@docusaurus/Head';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import HeroNetwork from '@site/src/components/HeroNetwork';
import Heading from '@theme/Heading';

import styles from './index.module.css';

const HOME_DESCRIPTION =
  'Test your AI agents against your own cases and criteria. Review failures and use evaluation results to gate releases with Agent Vigilo.';

const HOME_KEYWORDS = [
  'Agent Vigilo',
  'AI evaluation infrastructure',
  'generative AI evaluation',
  'LLM evaluation platform',
  'agent evaluation framework',
  'deployment gating',
  'CI evaluation gates',
  'WASM evaluators',
  'durable evaluation runs',
  'evaluator registry',
].join(', ');

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();

  return (
    <header className={styles.heroBanner}>
      <HeroNetwork />
      <div className={styles.heroShell}>
        <img className={styles.heroMark} src="/img/logo-dark.svg" width="188" height="112" alt="" aria-hidden="true" />
        <p className={styles.heroEyebrow}>AI evaluation and release gates</p>
        <Heading as="h1" className={styles.heroTitle}>
          {siteConfig.title}
        </Heading>
        <p className={styles.heroSubtitle}>Know what passes before you deploy.</p>
        <p className={styles.heroText}>
          Test your agents against your own cases and criteria.
          Review failures and use the results in your release pipeline.
        </p>
        <div className={styles.buttons}>
          <Link className="button button--primary" to="/docs/guides/getting-started">
            Get started
          </Link>
          <Link className={styles.secondaryButton} to="/docs/architecture/structure/">
            How it works
          </Link>
        </div>
        <p className={styles.projectDetails}>Open source. Self-hosted.</p>
      </div>
    </header>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();

  return (
    <Layout
      title="AI Evaluation Infrastructure"
      description={HOME_DESCRIPTION}>
      <Head>
        <meta name="keywords" content={HOME_KEYWORDS} />
        <meta property="og:title" content={`${siteConfig.title}: AI Evaluation Infrastructure`} />
        <meta property="og:description" content={HOME_DESCRIPTION} />
        <meta name="twitter:title" content={`${siteConfig.title}: AI Evaluation Infrastructure`} />
        <meta name="twitter:description" content={HOME_DESCRIPTION} />
      </Head>
      <HomepageHeader />
      <main>
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
