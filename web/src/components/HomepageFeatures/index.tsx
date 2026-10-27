import type {ReactNode} from 'react';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  label: string;
  title: string;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    label: '01',
    title: 'Set your criteria',
    description: (
      <>
        Choose what to test, how to score it, and what counts as a pass.
      </>
    ),
  },
  {
    label: '02',
    title: 'Run your evaluations',
    description: (
      <>
        Evaluate large datasets, track progress, and inspect or export
        individual results.
      </>
    ),
  },
  {
    label: '03',
    title: 'Check before release',
    description: (
      <>
        Use the final pass or fail result in CI. Incomplete evaluations
        keep the release gate closed.
      </>
    ),
  },
];

function Feature({label, title, description}: FeatureItem) {
  return (
    <article className={styles.feature}>
      <span className={styles.featureLabel}>{label}</span>
      <Heading as="h3" className={styles.featureTitle}>
        {title}
      </Heading>
      <p className={styles.featureText}>{description}</p>
    </article>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className={styles.inner}>
        <div className={styles.sectionHeader}>
          <span>What you get</span>
          <Heading as="h2">Evaluation infrastructure, not another score script.</Heading>
        </div>
        <div className={styles.featureGrid}>
          {FeatureList.map((props) => (
            <Feature key={props.label} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
