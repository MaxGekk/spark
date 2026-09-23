# The short post: the trailer for milestone 5's long read

*Drafted 23 September 2026. It lives in its own file rather than at the end of
`POST_MILESTONE_5.md`, because `dev/varka_post_page.py` publishes everything in
that file and a draft trailer was appearing on the page as if it were part of
the piece.*

## What it is for

The previous post (17 August 2026) put the whole argument in the post itself:
one dense paragraph, a link-preview card, no image. It reached about 15.5k
impressions and 81 reactions, and converted roughly 150 clicks into 16 stars -
a healthy star rate among visitors and about a 1% click-through. The reach was
never the problem. So this one is a trailer, on the owner's instruction kept to
performance alone: one allocation, two rates, one ratio, and the argument at
the other end of the link.

## The draft

> Spark computes `hour(t)` by building a `LocalTime`. Per row.
>
> Four divisions and an allocation, to read one field back out: 16.3
> nanoseconds a row over 500 million cached rows.
>
> Varka is a research fork of Apache Spark that compiles a whole projection
> into one vector loop - bytecode emitted with JDK 25's Class-File API, run
> over Arrow columns through the Vector API and Panama. The same `hour(t)`
> becomes one 64-bit division over eight lanes at a time: 1.0 nanosecond a row,
> and no object.
>
> On chained TIME expressions, three to five operations deep, it runs 31.8x
> faster than stock Spark 4.2 on a machine with a genuine 512-bit datapath -
> measured on GitHub Actions by a workflow anyone can dispatch, with the CPU
> and the datapath probe's own reading written into the results file.
>
> https://vecbricks.github.io/eight-rows-per-instruction/

**Image:** figure 2, the `LocalTime` per row against the one division
(`card.png` beside the published page, 1200x630).

**Alt text:** "A hand-drawn diagram. One TIME value, 52 349 000 000 000
nanoseconds since midnight, takes two paths: stock Spark builds a LocalTime
object with four fields and reads one of them, 16.3 nanoseconds a row; Varka
divides by 3 600 000 000 000, eight lanes at a time, 1.0 nanosecond a row."

## Mechanics, and the one thing to test

* **Post the image, not a link preview.** The previous post was a link card,
  which is the format that reaches fewest people and gives a reader the whole
  story without clicking.
* **The link is in the post above, and that is the arm to test.** Whether
  LinkedIn suppresses outbound links enough to justify moving it to the first
  comment is folklore in both directions; this project has two posts and can
  settle it with an A/B rather than an opinion. Put the link in the post this
  time, and in the first comment next time, and read GitHub's traffic API for
  referrals rather than guessing from impressions.
* **Snapshot the traffic first.** `gh api repos/vecbricks/varka/traffic/views`
  and `.../popular/referrers` keep only fourteen days, so take a reading before
  posting or the before-and-after cannot be recovered.
* **Same day, not the same hour, on the other channels.** A Show HN, r/java
  (the JDK 25 and Vector API angle is the draw there and nobody has told them)
  and r/apachespark reach engineers who do not read LinkedIn, and the
  reproduction recipe is what makes such a post survive scrutiny.

## What this post deliberately leaves out

Everything that is not a performance claim. The JVM-versus-native argument,
which the August post made and which is a post of its own. The emitter's
design, the fallback contract, and the coverage table. The finding that the
same twelve expressions read about 4x on a machine without AVX-512, because the
division lowering changes - it is the most interesting thing the milestone
measured and it is one scroll away in the long read, which is where a reader
who wants the caveat will look for it. And any question to the reader: the
post asks for a click and nothing else.
