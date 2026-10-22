/* Views for the Power BI review page and the steward scorecard. */
CREATE OR ALTER VIEW review.v_open_queue
AS
SELECT q.pair_id, q.review_type, q.band, q.confidence, q.priority, q.explanation, q.evidence,
       q.left_source, q.left_name, q.left_email_masked, q.left_phone_masked, q.left_city, q.left_postcode,
       q.right_source, q.right_name, q.right_email_masked, q.right_phone_masked, q.right_city, q.right_postcode,
       q.created_at, DATEDIFF(HOUR, q.created_at, SYSUTCDATETIME()) AS age_hours
FROM review.review_queue AS q
WHERE q.status IN (N'PENDING', N'HELD');
GO

CREATE OR ALTER VIEW review.v_steward_activity
AS
SELECT CAST(d.decided_at AS DATE) AS decision_date, d.decided_by, d.decision, COUNT(*) AS decisions
FROM review.decision_log AS d
GROUP BY CAST(d.decided_at AS DATE), d.decided_by, d.decision;
GO

CREATE OR ALTER VIEW review.v_queue_summary
AS
SELECT q.status, q.review_type, COUNT(*) AS items,
       SUM(CASE WHEN q.status = N'PENDING' AND q.created_at < DATEADD(HOUR, -48, SYSUTCDATETIME()) THEN 1 ELSE 0 END)
           AS pending_over_48h
FROM review.review_queue AS q
GROUP BY q.status, q.review_type;
GO
