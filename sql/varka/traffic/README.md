# Traffic snapshots

GitHub's traffic API keeps fourteen days and no more, so a reading that is not
taken is a reading that cannot be recovered. Each file here is one capture of
`repos/vecbricks/varka/traffic/{views,clones,popular/referrers,popular/paths}`
plus the star and fork counts, named for the moment it was taken.

They exist to answer one question honestly: what did a post actually do? The
August 2026 post reached about 15.5k impressions on LinkedIn and converted
roughly 150 clicks into 16 stars, and that is known only because the numbers
happened to be read within the window. Anything before it is gone.

Take one before publishing anything, and another a week after:

    dev/varka_traffic_snapshot.sh

The comparison that matters is not views but **referrals**: `linkedin.com` and
`com.linkedin.android` in `referrers`, against the change in `repo.stars`.
Views move with anything, including a crawler.
