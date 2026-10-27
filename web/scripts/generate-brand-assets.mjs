// logo.svg owns the mark geometry; all other brand images are derived from it.
// Run with ImageMagick and Liberation Sans installed: npm run brand:generate.
import {readFileSync, writeFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';

const directory = fileURLToPath(new URL('../static/img/', import.meta.url));
const source = readFileSync(`${directory}logo.svg`, 'utf8');
const ink = '#171717';
const paper = '#f2f0e9';
const paths = source.match(/<path\b[^>]*\/>/g)?.join('\n  ');
if (!paths || !source.includes('viewBox="0 0 460 274"') || !source.includes(`fill="${ink}"`)) {
  throw new Error('Expected the canonical 460 × 274 logo.svg mark and charcoal fill');
}

function write(name, content) {
  writeFileSync(`${directory}${name}`, content);
}

function render(args) {
  const result = spawnSync('magick', args, {cwd: directory, stdio: 'inherit'});
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`ImageMagick failed with status ${result.status}`);
}

write('logo-dark.svg', source.replace(`fill="${ink}"`, `fill="${paper}"`));
write('favicon.svg', `<svg xmlns="http://www.w3.org/2000/svg" width="256" height="256" viewBox="0 0 256 256" role="img" aria-label="Agent Vigilo">
  <circle cx="128" cy="128" r="128" fill="${ink}"/>
  <g transform="translate(27 68) scale(0.43913)" fill="${paper}">
  ${paths}
  </g>
</svg>
`);
write('social-card.svg', `<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630" role="img" aria-label="Agent Vigilo — distributed evaluation infrastructure">
  <rect width="1200" height="630" fill="${ink}"/>
  <g transform="translate(439 118) scale(0.7)" fill="${paper}">
  ${paths}
  </g>
  <text x="600" y="423" text-anchor="middle" font-family="Liberation Sans" font-size="72" font-weight="600" letter-spacing="-2" fill="${paper}">Agent Vigilo</text>
  <text x="600" y="483" text-anchor="middle" font-family="Liberation Sans" font-size="24" fill="#b7b5af">Distributed evaluation infrastructure</text>
</svg>
`);

render(['-background', 'none', '-density', '384', 'favicon.svg', '-define', 'icon:auto-resize=64,48,32,24,16', '-depth', '8', '-strip', 'favicon.ico']);
render(['-background', ink, '-density', '384', 'favicon.svg', '-resize', '180x180', '-depth', '8', '-strip', 'apple-touch-icon.png']);
render(['-background', 'none', '-density', '192', 'social-card.svg', '-resize', '1200x630', '-depth', '8', '-strip', 'social-card.png']);
