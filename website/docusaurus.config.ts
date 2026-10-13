import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'diapason',
  tagline: 'An open-loop load generator whose measurements can be trusted',
  favicon: 'img/favicon.svg',

  future: {
    v4: true,
  },

  url: 'https://menraromial.github.io',
  baseUrl: '/diapason/',
  organizationName: 'menraromial',
  projectName: 'diapason',
  trailingSlash: false,

  onBrokenLinks: 'throw',

  // .md files are CommonMark, not MDX: the reference pages come from the
  // README, which uses braces and angle brackets freely.
  markdown: {
    format: 'detect',
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'diapason',
      logo: {
        alt: 'diapason',
        src: 'img/logo.svg',
      },
      items: [
        {type: 'docSidebar', sidebarId: 'docs', position: 'left', label: 'Documentation'},
        {to: '/docs/installation', label: 'Install', position: 'left'},
        {
          href: 'https://github.com/menraromial/diapason/releases',
          label: 'Releases',
          position: 'right',
        },
        {
          href: 'https://github.com/menraromial/diapason',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {label: 'Introduction', to: '/docs/intro'},
            {label: 'Accounting model', to: '/docs/accounting-model'},
            {label: 'Outputs', to: '/docs/outputs'},
          ],
        },
        {
          title: 'Project',
          items: [
            {label: 'GitHub', href: 'https://github.com/menraromial/diapason'},
            {label: 'Releases', href: 'https://github.com/menraromial/diapason/releases'},
            {label: 'Changelog', href: 'https://github.com/menraromial/diapason/blob/main/CHANGELOG.md'},
          ],
        },
      ],
      copyright: `diapason is distributed under the MIT or Apache-2.0 license.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['toml', 'rust', 'bash', 'json'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
