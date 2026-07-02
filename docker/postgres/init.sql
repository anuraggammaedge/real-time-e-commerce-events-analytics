CREATE TABLE IF NOT EXISTS events_per_minute (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    count BIGINT,
    PRIMARY KEY (window_start, window_end)
);

CREATE TABLE IF NOT EXISTS category_metrics (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    category TEXT,
    count BIGINT,
    PRIMARY KEY (window_start, window_end, category)
);
