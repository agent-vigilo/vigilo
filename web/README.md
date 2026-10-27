# Documentation Website

The Docusaurus site at [agentvigilo.com](https://agentvigilo.com) is the public
documentation source of truth. Pages live in `docs/`; architecture diagram sources
live beside their pages as `.mmd` files. Shared glossary definitions live in
`src/data/glossary.ts`.

## Development

From the repository root, use Node.js 24 and the checked-in npm lockfile:

```bash
npm --prefix web ci
npm --prefix web start
```

The development server reloads pages as they change. Keep page titles, sidebar
labels, and visible headings consistent. Document behavior in present tense and
update paired diagrams when flows or ownership change. Repository READMEs link
to the canonical guides instead of duplicating them.

## Brand Assets

Brand assets live in `static/img/`. `logo.svg` owns the mark geometry;
`logo-dark.svg` supplies the ivory variant for dark surfaces. The README and
navigation select the appropriate variant, while the homepage uses the ivory
mark on charcoal. Favicons and the social sharing card use the same geometry.

After editing the source mark, regenerate the derived assets with ImageMagick 7
(`magick`) and the Liberation Sans font installed:

```bash
npm --prefix web run brand:generate
```

Generated SVG, PNG, and ICO assets are checked in; the website build needs no
image tools or system fonts.

The homepage's decorative network lives in `src/components/HeroNetwork/`.
Eight translucent planes frame a clear area for the headline and calls to action.
A brief left-to-right highlight repeats every 14 seconds to suggest inference;
this is an abstract visual, not a model simulation. CSS scales the composition to
the viewport, with a gentler angle on mobile. Animation pauses offscreen and in
hidden tabs; reduced-motion preferences leave the static lighting and planes.

## Verification

```bash
npm --prefix web run typecheck
npm --prefix web run build
```

The build checks documentation links and validates the generated sitemap.
Output is written to `web/build`. Preview it with:

```bash
npm --prefix web run serve
```

## Deployment

[Deploy Web](../.github/workflows/deploy-web.yaml) builds the site on a published
release or manual workflow dispatch, uploads static files to S3, and invalidates
the CloudFront distribution. The workflow assumes the configured AWS role through
GitHub OIDC. HTML and static assets use separate cache policies.
