# docs

Operational guides for mast. Prose only — no code, no config, no build.

## What belongs here, and what does not

| | Lives in |
| --- | --- |
| **Why the system is shaped this way** | `mastmq/mast/docs/ARCHITECTURE.md` — next to the code, because it changes when the code does |
| **How to operate it, migrate to it, or reason about what it promises** | here |
| **What it currently does and does not do** | the `mastmq/mast` README |

A guide that goes stale the moment someone edits a Go file belongs in the broker repo. A guide that a reader needs before they have cloned anything belongs here.

## Adding a guide

Two things, in the same commit:

1. The file under `guides/`.
2. A row in the table in `README.md`. A guide nothing links to is a guide nobody reads — it is not in the sitemap, not on the website, and not in the broker README.

Then check whether it should also be linked from `mastmq/mast/README.md` or from `src/data/site.ts` on the website. `migrating-from-emqx.md` and `delivery-guarantees.md` are both linked from all three.

## The two guides now

**`migrating-from-emqx.md`** — what maps across, what does not, and the traps found doing it for real. Grounded in an actual EMQX replacement, not in reading EMQX's docs.

**`delivery-guarantees.md`** — what each QoS actually promises hop by hop. This one is load-bearing: it is the public answer to the sharpest criticism the project has received, and it is linked from the broker README's status line, the cross-node bullet and the website.

Its claims are checkable against code, and were checked when written: zero KV operations on a non-retained publish, zero round trips on the fabric hop, and the ingress node acking before the at-most-once hop. **If the broker's delivery path changes, this guide is wrong until it is edited.** `mast/docs/ARCHITECTURE.md` and the broker README move with it.

## Tone

These guides say what is true, including when it is unflattering. "QoS 1 within a node, best-effort across nodes" is stated plainly, with the conditions under which it actually drops, and the fix that is designed but not built. Do not soften a limitation into a roadmap item — the credibility of the whole set rests on the reader believing the uncomfortable parts.

Give numbers where numbers exist and say where they came from. Say when something is a choice other brokers make differently rather than implying there is one right answer.

## Conventions

Markdown one paragraph per line; never hard-wrap prose. Conventional commits (`docs:`). Links to the broker use absolute GitHub URLs, because this README is also rendered on the org profile and on the website.
