import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'Agent Vigilo',
  tagline: 'Distributed Evaluation and Deployment Gating for Generative AI Systems',
  favicon: 'img/favicon.svg',
  headTags: [
    {
      tagName: 'link',
      attributes: {rel: 'alternate icon', type: 'image/x-icon', href: '/img/favicon.ico'},
    },
    {
      tagName: 'link',
      attributes: {rel: 'apple-touch-icon', sizes: '180x180', href: '/img/apple-touch-icon.png'},
    },
  ],

  // Future flags, see https://docusaurus.io/docs/api/docusaurus-config#future
  future: {
    v4: true, // Improve compatibility with the upcoming Docusaurus v4
  },

  // Set the production url of your site here
  url: 'https://agentvigilo.com',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'agent-vigilo', // Usually your GitHub org/user name.
  projectName: 'agent-vigilo', // Usually your repo name.

  onBrokenLinks: 'throw',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    mermaid: true,
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/gregl83/agent-vigilo/tree/main/web/docs/',
        },
        blog: false,
        sitemap: {
          changefreq: null,
          ignorePatterns: ['/search', '/docs/category/guides'],
          priority: null,
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themes: [
    '@docusaurus/theme-mermaid',
    [
      '@easyops-cn/docusaurus-search-local',
      {
        hashed: true,
        indexDocs: true,
        indexBlog: false,
        indexPages: true,
        language: ['en'],
      },
    ],
  ],

  plugins: [
    function mermaidSourceLoader() {
      return {
        name: 'mermaid-source-loader',
        configureWebpack() {
          return {
            module: {
              rules: [
                {
                  test: /\.mmd$/i,
                  type: 'asset/source',
                },
              ],
            },
          };
        },
      };
    },
  ],

  themeConfig: {
    image: 'img/social-card.png',
    metadata: [
      {
        name: 'keywords',
        content:
          'Agent Vigilo, AI evaluation infrastructure, generative AI evaluation, LLM evaluation, agent evaluation, deployment gating, CI gates, WASM evaluators, durable evaluation runs, evaluator registry',
      },
      {
        property: 'og:type',
        content: 'website',
      },
      {
        name: 'twitter:card',
        content: 'summary_large_image',
      },
    ],
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Agent Vigilo',
      logo: {
        alt: 'Agent Vigilo',
        src: 'img/logo.svg',
        srcDark: 'img/logo-dark.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'tutorialSidebar',
          position: 'left',
          label: 'Guides',
        },
        {
          to: '/docs/glossary',
          label: 'Glossary',
          position: 'left',
        },
        {
          href: 'https://github.com/gregl83/agent-vigilo',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Getting Started',
              to: '/docs/guides/getting-started',
            },
            {
              label: 'Architecture',
              to: '/docs/architecture/structure/context/',
            },
            {
              label: 'Glossary',
              to: '/docs/glossary',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'Stack Overflow',
              href: 'https://stackoverflow.com/questions/tagged/agent-vigilo',
            },
            {
              label: 'X',
              href: 'https://x.com/agentvigilo',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/gregl83/agent-vigilo',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Agent Vigilo, Inc.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
