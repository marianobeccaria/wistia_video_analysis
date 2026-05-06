# Athena Reporting Queries

This document contains Athena queries for the Wistia gold analytics model.

Glue database:

```txt
wistia_video_analytics
```

Gold tables:

```txt
dim_media
dim_visitor
fact_media_engagement
```

The fact table grain is one row per `media_id`, `visitor_id`, and `date`.

## 1. Smoke Test Gold Tables

Use these queries first to confirm Athena can read the gold layer.

```sql
SELECT *
FROM wistia_video_analytics.dim_media
LIMIT 10;
```

```sql
SELECT *
FROM wistia_video_analytics.dim_visitor
LIMIT 10;
```

```sql
SELECT *
FROM wistia_video_analytics.fact_media_engagement
LIMIT 10;
```

## 2. Plays By Date And Channel

Shows daily plays for Facebook and YouTube.

```sql
SELECT
    f.date,
    m.channel,
    SUM(f.play_count) AS total_plays
FROM wistia_video_analytics.fact_media_engagement f
JOIN wistia_video_analytics.dim_media m
    ON f.media_id = m.media_id
GROUP BY
    f.date,
    m.channel
ORDER BY
    f.date,
    m.channel;
```

## 3. Watch Time By Date And Channel

Shows total estimated visitor watch time by day and channel.

```sql
SELECT
    f.date,
    m.channel,
    ROUND(SUM(f.total_watch_time_hours), 4) AS total_watch_time_hours,
    ROUND(SUM(f.total_watch_time_seconds), 2) AS total_watch_time_seconds
FROM wistia_video_analytics.fact_media_engagement f
JOIN wistia_video_analytics.dim_media m
    ON f.media_id = m.media_id
GROUP BY
    f.date,
    m.channel
ORDER BY
    f.date,
    m.channel;
```

## 4. Average Watched Percent By Media

Shows average watched percent by video. This helps compare how deeply viewers watched each media asset.

```sql
SELECT
    m.channel,
    f.media_id,
    m.title,
    ROUND(AVG(f.watched_percent), 4) AS avg_watched_percent,
    ROUND(MAX(f.watched_percent), 4) AS max_watched_percent,
    COUNT(DISTINCT f.visitor_id) AS unique_visitors,
    SUM(f.play_count) AS total_plays
FROM wistia_video_analytics.fact_media_engagement f
JOIN wistia_video_analytics.dim_media m
    ON f.media_id = m.media_id
GROUP BY
    m.channel,
    f.media_id,
    m.title
ORDER BY
    avg_watched_percent DESC;
```

## 5. Unique Visitors By Channel And Media

Shows unique visitor counts at channel and media level.

```sql
SELECT
    m.channel,
    f.media_id,
    m.title,
    COUNT(DISTINCT f.visitor_id) AS unique_visitors,
    SUM(f.play_count) AS total_plays
FROM wistia_video_analytics.fact_media_engagement f
JOIN wistia_video_analytics.dim_media m
    ON f.media_id = m.media_id
GROUP BY
    m.channel,
    f.media_id,
    m.title
ORDER BY
    unique_visitors DESC;
```

## 6. Facebook Vs YouTube Summary

Compares the main performance metrics by channel.

```sql
SELECT
    m.channel,
    COUNT(DISTINCT f.media_id) AS media_count,
    COUNT(DISTINCT f.visitor_id) AS unique_visitors,
    SUM(f.play_count) AS total_plays,
    ROUND(AVG(f.play_rate), 4) AS avg_play_rate,
    ROUND(AVG(f.watched_percent), 4) AS avg_watched_percent,
    ROUND(SUM(f.total_watch_time_hours), 4) AS total_watch_time_hours
FROM wistia_video_analytics.fact_media_engagement f
JOIN wistia_video_analytics.dim_media m
    ON f.media_id = m.media_id
GROUP BY
    m.channel
ORDER BY
    total_plays DESC;
```

## 7. Daily Facebook Vs YouTube Trend

Shows a side-by-side daily trend for plays and watch time.

```sql
SELECT
    f.date,
    SUM(CASE WHEN m.channel = 'Facebook' THEN f.play_count ELSE 0 END) AS facebook_plays,
    SUM(CASE WHEN m.channel = 'YouTube' THEN f.play_count ELSE 0 END) AS youtube_plays,
    ROUND(SUM(CASE WHEN m.channel = 'Facebook' THEN f.total_watch_time_hours ELSE 0 END), 4) AS facebook_watch_hours,
    ROUND(SUM(CASE WHEN m.channel = 'YouTube' THEN f.total_watch_time_hours ELSE 0 END), 4) AS youtube_watch_hours
FROM wistia_video_analytics.fact_media_engagement f
JOIN wistia_video_analytics.dim_media m
    ON f.media_id = m.media_id
GROUP BY
    f.date
ORDER BY
    f.date;
```

## 8. Top Visitor Locations

Shows where identified viewers are coming from. Visitor IDs are hashed in gold; raw visitor keys, IPs, and emails are not exposed.

```sql
SELECT
    country,
    region,
    city,
    COUNT(DISTINCT visitor_id) AS unique_visitors,
    SUM(event_count) AS total_events,
    ROUND(AVG(avg_percent_viewed), 4) AS avg_watched_percent
FROM wistia_video_analytics.dim_visitor
WHERE country IS NOT NULL
GROUP BY
    country,
    region,
    city
ORDER BY
    unique_visitors DESC
LIMIT 25;
```

## 9. Device And Platform Breakdown

Shows engagement by browser, platform, and mobile flag.

```sql
SELECT
    platform,
    browser,
    mobile,
    COUNT(DISTINCT visitor_id) AS unique_visitors,
    SUM(event_count) AS total_events,
    ROUND(AVG(avg_percent_viewed), 4) AS avg_watched_percent
FROM wistia_video_analytics.dim_visitor
GROUP BY
    platform,
    browser,
    mobile
ORDER BY
    unique_visitors DESC;
```

## 10. Date-Filtered Query For Partition Pruning

`fact_media_engagement` uses partition projection on `date` and `media_id`. Add date filters when possible so Athena scans less data.

```sql
SELECT
    f.date,
    m.channel,
    f.media_id,
    SUM(f.play_count) AS total_plays,
    ROUND(SUM(f.total_watch_time_hours), 4) AS total_watch_time_hours
FROM wistia_video_analytics.fact_media_engagement f
JOIN wistia_video_analytics.dim_media m
    ON f.media_id = m.media_id
WHERE f.date BETWEEN DATE '2026-05-01' AND DATE '2026-05-07'
GROUP BY
    f.date,
    m.channel,
    f.media_id
ORDER BY
    f.date,
    m.channel,
    f.media_id;
```

## Notes

- `play_count` in `fact_media_engagement` is calculated at visitor-day-media grain from distinct Wistia event keys.
- `media_play_count`, `media_load_count`, and `media_hours_watched` come from Wistia daily media stats and are repeated across visitor rows for the same media/date.
- For channel-level totals, use `SUM(f.play_count)` for visitor-level plays.
- For media-level Wistia aggregate metrics, query by `media_id` and `date` carefully to avoid double-counting repeated daily media stats across visitors.
