import ErrorBoundary from '@docusaurus/ErrorBoundary';
import {ErrorBoundaryErrorMessageFallback} from '@docusaurus/theme-common';
import {
  MermaidContainerClassName,
  useMermaidConfig,
  useMermaidRenderResult,
} from '@docusaurus/theme-mermaid/client';
import type {Props} from '@theme/Mermaid';
import type {RenderResult} from 'mermaid';
import React, {
  type ReactNode,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';

import DiagramViewport from '@site/src/components/DiagramViewport';

let iconRegistrationPromise: Promise<void> | undefined;

function registerFontAwesomeIcons(): Promise<void> {
  iconRegistrationPromise ??= Promise.all([
    import('mermaid'),
    import('@iconify-json/fa6-solid/icons.json'),
  ]).then(([mermaidModule, fa6SolidIcons]) => {
    mermaidModule.default.registerIconPacks([
      {name: 'fa', icons: fa6SolidIcons.default},
      {name: 'fas', icons: fa6SolidIcons.default},
    ]);
  });

  return iconRegistrationPromise;
}

function MermaidRenderResult({
  renderResult,
}: {
  renderResult: RenderResult;
}): ReactNode {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const div = ref.current;
    if (div) {
      renderResult.bindFunctions?.(div);
    }
  }, [renderResult]);

  const diagram = (
    <div
      ref={ref}
      className={MermaidContainerClassName}
      // Mermaid returns a complete SVG string and optional click binders.
      // eslint-disable-next-line react/no-danger
      dangerouslySetInnerHTML={{__html: renderResult.svg}}
    />
  );

  return <DiagramViewport>{diagram}</DiagramViewport>;
}

function MermaidRenderer({value}: Props): ReactNode {
  const defaultMermaidConfig = useMermaidConfig();
  const mermaidConfig = useMemo(() => {
    const colors = getComputedStyle(document.documentElement);
    const color = (name: string) => colors.getPropertyValue(name).trim();
    return {
      ...defaultMermaidConfig,
      theme: 'base' as const,
      themeVariables: {
        ...defaultMermaidConfig.themeVariables,
        background: color('--diagram-background'),
        primaryColor: color('--diagram-component-fill'),
        primaryTextColor: color('--diagram-component-text'),
        primaryBorderColor: color('--diagram-component-stroke'),
        secondaryColor: color('--ifm-color-emphasis-100'),
        tertiaryColor: color('--diagram-background'),
        lineColor: color('--ifm-color-emphasis-600'),
        textColor: color('--ifm-font-color-base'),
        clusterBkg: color('--diagram-background'),
        clusterBorder: color('--ifm-color-emphasis-400'),
        edgeLabelBackground: color('--diagram-background'),
        noteBkgColor: color('--diagram-background'),
        noteTextColor: color('--ifm-font-color-base'),
        actorBkg: color('--diagram-component-fill'),
        actorTextColor: color('--diagram-component-text'),
        fontFamily: color('--ifm-font-family-base'),
      },
      flowchart: {...defaultMermaidConfig.flowchart, useMaxWidth: false},
    };
  }, [defaultMermaidConfig]);

  const renderResult = useMermaidRenderResult({
    config: mermaidConfig,
    text: value,
  });
  if (renderResult === null) {
    return null;
  }

  return <MermaidRenderResult renderResult={renderResult} />;
}

function MermaidWithIcons(props: Props): ReactNode {
  const [iconsReady, setIconsReady] = useState(false);

  useEffect(() => {
    let mounted = true;

    registerFontAwesomeIcons()
      .catch((error: unknown) => {
        console.error('Failed to register Mermaid Font Awesome icons', error);
      })
      .finally(() => {
        if (mounted) {
          setIconsReady(true);
        }
      });

    return () => {
      mounted = false;
    };
  }, []);

  if (!iconsReady) {
    return null;
  }

  return <MermaidRenderer {...props} />;
}

export default function Mermaid(props: Props): ReactNode {
  return (
    <ErrorBoundary
      fallback={(params) => <ErrorBoundaryErrorMessageFallback {...params} />}>
      <MermaidWithIcons {...props} />
    </ErrorBoundary>
  );
}
