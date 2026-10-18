CREATE TABLE IF NOT EXISTS cleaned_clickstream(
    event_id     TEXT NOT NULL,
    user_id      TEXT,
    session_id   TEXT NOT NULL,
    event_type   TEXT,
    timestamp    TIMESTAMPTZ,
    product_id   TEXT,
    product_name TEXT,
    brand        TEXT,
    category     TEXT,
    price        DOUBLE PRECISION  
);