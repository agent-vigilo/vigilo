import type {ReactNode} from 'react';
import DocBreadcrumbs from '@theme-original/DocBreadcrumbs';
import {DocVersionPicker} from '@site/src/components/DocVersions';
import styles from './styles.module.css';

export default function VersionedDocBreadcrumbs(): ReactNode {
  return (
    <div className={styles.header}>
      <DocBreadcrumbs />
      <DocVersionPicker />
    </div>
  );
}
