SELECT
    category,
    SUM(count) AS total_events
FROM category_metrics
GROUP BY category
ORDER BY total_events DESC;