
-- Executive monthly KPI view
WITH matter_base AS (
    SELECT
        m.matter_id,
        m.lead_id,
        DATE_TRUNC('month', m.opened_date) AS opened_month,
        m.status,
        m.fee_revenue,
        l.retained
    FROM matters m
    LEFT JOIN leads l
      ON m.lead_id = l.lead_id
),
monthly AS (
    SELECT
        opened_month,
        COUNT(DISTINCT lead_id) AS leads,
        SUM(CASE WHEN retained THEN 1 ELSE 0 END) AS retained_cases,
        SUM(CASE WHEN status = 'Closed Won' THEN 1 ELSE 0 END) AS closed_won,
        SUM(fee_revenue) AS fee_revenue
    FROM matter_base
    GROUP BY 1
),
marketing AS (
    SELECT
        DATE_TRUNC('month', date) AS month,
        SUM(spend) AS marketing_spend
    FROM marketing_spend
    GROUP BY 1
)
SELECT
    m.opened_month,
    m.leads,
    m.retained_cases,
    m.closed_won,
    m.fee_revenue,
    mk.marketing_spend,
    CASE WHEN m.retained_cases > 0 THEN mk.marketing_spend / m.retained_cases END AS cost_per_retained_case,
    CASE WHEN m.leads > 0 THEN m.fee_revenue / m.leads END AS revenue_per_lead
FROM monthly m
LEFT JOIN marketing mk
  ON m.opened_month = mk.month
ORDER BY 1;
