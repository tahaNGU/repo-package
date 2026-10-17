-- Index for ordered delivery (postgres.WithOrdering). It supports the
-- "is an earlier message for this key still unpublished?" test that serialises
-- each ordering key. Apply it only if you use ordered delivery: it adds write
-- cost to every insert and state change of a keyed message.
CREATE INDEX IF NOT EXISTS {{prefix}}_ordering_idx
    ON {{table}} (ordering_key, seq)
    WHERE state <> 'published' AND ordering_key IS NOT NULL;
