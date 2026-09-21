CREATE TABLE raw_clickstream(
    event_id    TEXT,
    user_id     TEXT,
    session_id  TEXT,
    event_type  TEXT,
    timestamp   TIMESTAMPTZ,
    product_id  TEXT,
    product_name TEXT,
    category    TEXT,
    price       TEXT
);