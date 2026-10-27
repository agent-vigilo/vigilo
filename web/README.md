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
