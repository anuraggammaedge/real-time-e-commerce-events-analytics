DROP TABLE IF EXISTS events_per_minute;

CREATE TABLE events_per_minute
(
    window_start TIMESTAMPTZ,
    window_end   TIMESTAMPTZ,
    count        BIGINT
);

DROP TABLE IF EXISTS category_metrics;

CREATE TABLE category_metrics
(
    window_start TIMESTAMPTZ,
    window_end   TIMESTAMPTZ,
    category     TEXT,
    count        BIGINT
);