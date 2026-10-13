import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docs: [
    {
      type: 'category',
      label: 'Getting started',
      collapsed: false,
      items: ['intro', 'installation', 'quick-start', 'guides/first-run'],
    },
    {
      type: 'category',
      label: 'Guides',
      collapsed: false,
      items: [
        'guides/coordinated-omission',
        'guides/finding-capacity',
        'guides/client-limits',
        'guides/failures',
        'guides/realistic-requests',
        'guides/http2-and-tls',
        'guides/ci-gates',
        'guides/analysing-results',
        'guides/live-metrics',
        'guides/distributed',
        'guides/reference-server',
        'guides/cookbook',
      ],
    },
    {
      type: 'category',
      label: 'Concepts',
      collapsed: false,
      items: ['accounting-model', 'fidelity-checks'],
    },
    {
      type: 'category',
      label: 'Reference',
      collapsed: false,
      items: ['configuration', 'command-line', 'outputs', 'library'],
    },
    {
      type: 'category',
      label: 'Validation',
      collapsed: true,
      items: ['validation', 'design-measurements', 'status'],
    },
    'license',
  ],
};

export default sidebars;
