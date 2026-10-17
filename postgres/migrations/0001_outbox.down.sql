-- Removes the outbox table and everything that belongs to it. This destroys
-- undelivered messages; drain the outbox first.
DROP TABLE IF EXISTS {{table}};
