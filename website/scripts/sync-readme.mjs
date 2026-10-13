// Splits the repository README into documentation pages, one per "## "
// section, so that the README stays the single source of the reference.
// Runs before `npm start` and `npm run build`; the pages it writes are not
// committed.
import {readFileSync, writeFileSync, mkdirSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const readme = readFileSync(join(here, '..', '..', 'README.md'), 'utf8');
const docs = join(here, '..', 'docs');

// README section title -> page id and sidebar label.
const pages = {
  'Status': ['status', 'Status'],
  'Installation': ['installation', 'Installation'],
  'Quick start': ['quick-start', 'Quick start'],
  'The accounting model': ['accounting-model', 'Accounting model'],
  'Fidelity checks': ['fidelity-checks', 'Fidelity checks'],
  'Output': ['outputs', 'Outputs'],
  'Configuration': ['configuration', 'Configuration'],
  'Command line': ['command-line', 'Command line'],
  'Library use': ['library', 'Library'],
  'Validation': ['validation', 'Validation tests'],
  'Design measurements': ['design-measurements', 'Design measurements'],
  'License': ['license', 'License'],
};

// Split outside code fences only.
const sections = [];
let current = null;
let fence = false;
for (const line of readme.split('\n')) {
  if (line.startsWith('```')) fence = !fence;
  if (!fence && line.startsWith('## ')) {
    current = {title: line.slice(3).trim(), lines: []};
    sections.push(current);
    continue;
  }
  if (current) current.lines.push(line);
}

mkdirSync(docs, {recursive: true});
const seen = new Set();
for (const s of sections) {
  const page = pages[s.title];
  if (!page) {
    throw new Error(`README section "${s.title}" has no documentation page; add it to scripts/sync-readme.mjs`);
  }
  const [id, label] = page;
  seen.add(s.title);
  // Headings move up one level: the section title is the page title.
  let inFence = false;
  const body = s.lines
    .map((l) => {
      if (l.startsWith('```')) inFence = !inFence;
      return !inFence && /^#{3,6} /.test(l) ? l.slice(1) : l;
    })
    .join('\n')
    .trim();
  const front = `---\nid: ${id}\ntitle: ${JSON.stringify(s.title === 'Output' ? 'Outputs' : s.title)}\nsidebar_label: ${JSON.stringify(label)}\ncustom_edit_url: https://github.com/menraromial/diapason/blob/main/README.md\n---\n\n`;
  writeFileSync(join(docs, `${id}.md`), front + body + '\n');
}
for (const title of Object.keys(pages)) {
  if (!seen.has(title)) throw new Error(`README has no section "${title}"`);
}
console.log(`sync-readme: ${sections.length} pages written from README.md`);
