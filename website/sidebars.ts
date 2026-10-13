import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docs: [
    {
      type: 'category',
      label: 'Getting started',
      collapsed: false,
      items: ['intro', 'installation', 'quick-start'],
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
      collapsed: false,
      items: ['validation', 'design-measurements', 'status'],
    },
    'license',
  ],
};

export default sidebars;
