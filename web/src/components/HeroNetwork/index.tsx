import {useEffect, useRef, useState, type CSSProperties} from 'react';
import styles from './styles.module.css';

const layers = Array.from({length: 8}, (_, index) => index);

/** Abstract neural layers frame the hero; a quiet light pass suggests inference. */
export default function HeroNetwork() {
  const ref = useRef<HTMLDivElement>(null);
  const [active, setActive] = useState(false);

  useEffect(() => {
    const element = ref.current;
    if (!element) return;
    let visible = false;
    const update = () => setActive(visible && document.visibilityState === 'visible');
    const observer = new IntersectionObserver(([entry]) => {
      visible = entry.isIntersecting;
      update();
    });
    observer.observe(element);
    document.addEventListener('visibilitychange', update);
    return () => {
      observer.disconnect();
      document.removeEventListener('visibilitychange', update);
    };
  }, []);

  return (
    <div ref={ref} className={styles.network} data-active={active} aria-hidden="true">
      <div className={styles.field}>
        {layers.map(layer => (
          <div
            key={layer}
            className={styles.layer}
            style={{'--layer': layer, '--activation-delay': `${0.7 + layer * 0.22}s`} as CSSProperties}>
            <div className={styles.activation} />
          </div>
        ))}
      </div>
    </div>
  );
}
