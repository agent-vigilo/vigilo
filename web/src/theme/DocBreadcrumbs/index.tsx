import type {ReactNode} from 'react';
import DocBreadcrumbs from '@theme-original/DocBreadcrumbs';
import {DocReleaseMetadata, DocVersionPicker} from '@site/src/components/DocVersions';
import styles from './styles.module.css';

export default function VersionedDocBreadcrumbs(): ReactNode {
  return (
    <div className={styles.header}>
      <DocReleaseMetadata />
      <DocBreadcrumbs />
      <DocVersionPicker />
    </div>
  );
}
