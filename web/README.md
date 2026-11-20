# Documentation Website

The Docusaurus site at [agentvigilo.com](https://agentvigilo.com) is the public
documentation source of truth. Pages live in `docs/`; architecture diagram sources
live beside their pages as `.mmd` files. Shared glossary definitions live in
`src/data/glossary.ts`.

Downloadable case-storage examples live in `../vigilo/fixtures/examples/case-storage/`
and are served at `/examples/case-storage/`. The crate's contract tests use these
same packaged files; keep a single copy of each example.

## Development

From the repository root, use Node.js 24 and the checked-in npm lockfile:

```bash
npm --prefix web ci
npm --prefix web start
```

The development server reloads pages as they change. Open a docs page and use
the **Unreleased** version control next to the breadcrumbs. Published docs
display the exact release and load available choices from the release manifest.

Keep page titles, sidebar
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
npm --prefix web run test:docs
npm --prefix web run build
VIGILO_DOCS_RELEASE=0.1.0 VIGILO_DOCS_ARCHIVE=1 npm --prefix web run build
```

The build checks documentation links and validates the generated sitemap.
Output is written to `web/build`. Preview it with:

```bash
npm --prefix web run serve
```

## Documentation Releases

`/docs` serves the latest published Vigilo release. Each exact release has a
rendered snapshot under `/releases/<version>/docs/` with its own navigation,
diagrams, examples, and search index. The docs picker lists the latest patch
of each visible major release line from v1 onward. During beta, the picker lists
only the latest release. Release History links to exact snapshots, including
earlier beta releases.

[Publish Vigilo Crate](../.github/workflows/publish-crate.yaml) calls
[Deploy Web](../.github/workflows/deploy-web.yaml) after verifying the
published crate's checksum. Build validates the current site and archive with
the version from the crate manifest. Deploy Web confirms publication and builds
with the tagged commit's npm lockfile. It writes the snapshot's identity marker after its
files upload and updates `docs-versions.json` last. The default docs move
forward in release order. An older-line patch updates its line in the picker
and leaves the default docs on the newer release.

The workflow uses the configured AWS role through GitHub OIDC. It needs
`s3:GetObject` for the release manifest and snapshot markers, the existing
site upload permissions, and CloudFront invalidation access, including
`cloudfront:GetInvalidation`. The workflow waits for invalidation and checks the
public manifest, exact snapshot, and live docs before it succeeds. The static
site routes directory URLs to their `index.html` under both `/docs/` and
`/releases/`. The root site sync excludes that archive prefix.

### Manual deployment

- To retry a release, run **Deploy Web** with its published `vigilo/vX.Y.Z`
  tag. A completed snapshot is reused after its identity is verified.
- To correct the latest docs or update the marketing pages, supply
  `docs_correction_sha`. The commit must descend from the latest release
  tag and differ only under `web/`. This rebuilds the live site and preserves
  the tagged snapshot.
- To retire a past line from the picker, add the line to
  `docs-release-policy.json` and run **Deploy Web** with
  `apply_archive_policy` enabled. Release History and existing snapshot links
  remain available.
