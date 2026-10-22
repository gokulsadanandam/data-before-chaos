/* ---------------------------------------------------------------------------------------
   review.usp_merge_review_queue
   Called by the pipeline right after the Copy activity loads review.review_queue_stage.

   * new pairs                        -> inserted as PENDING
   * pairs still pending              -> details refreshed (score, explanation, priority)
   * decided / held pairs             -> left alone (a decision is never overwritten)
   * pending pairs no longer proposed -> SUPERSEDED (the data changed; nothing to decide)
--------------------------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE review.usp_merge_review_queue
    @run_id NVARCHAR(64)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;

    MERGE review.review_queue WITH (HOLDLOCK) AS tgt
    USING (
        SELECT *
        FROM (
            SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.pair_id ORDER BY s.created_at DESC) AS rn
            FROM review.review_queue_stage AS s
        ) AS d
        WHERE d.rn = 1
    ) AS src
        ON tgt.pair_id = src.pair_id
    WHEN MATCHED AND tgt.status = N'PENDING' THEN
        UPDATE SET
            tgt.review_type       = src.review_type,
            tgt.confidence        = src.confidence,
            tgt.band              = src.band,
            tgt.explanation       = src.explanation,
            tgt.evidence          = src.evidence,
            tgt.priority          = COALESCE(src.priority, 0),
            tgt.left_name         = src.left_name,
            tgt.right_name        = src.right_name,
            tgt.left_email_masked = src.left_email_masked,
            tgt.right_email_masked = src.right_email_masked,
            tgt.left_phone_masked = src.left_phone_masked,
            tgt.right_phone_masked = src.right_phone_masked,
            tgt.left_city         = src.left_city,
            tgt.right_city        = src.right_city,
            tgt.left_postcode     = src.left_postcode,
            tgt.right_postcode    = src.right_postcode,
            tgt.last_seen_run_id  = @run_id,
            tgt.updated_at        = SYSUTCDATETIME()
    WHEN MATCHED AND tgt.status = N'SUPERSEDED' THEN
        UPDATE SET tgt.status = N'PENDING', tgt.last_seen_run_id = @run_id, tgt.updated_at = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (pair_id, review_type, left_id, right_id, left_source, left_name, left_email_masked,
                left_phone_masked, left_city, left_postcode, right_source, right_name, right_email_masked,
                right_phone_masked, right_city, right_postcode, confidence, band, explanation, evidence,
                priority, status, first_seen_run_id, last_seen_run_id)
        VALUES (src.pair_id, src.review_type, src.left_id, src.right_id, src.left_source, src.left_name,
                src.left_email_masked, src.left_phone_masked, src.left_city, src.left_postcode, src.right_source,
                src.right_name, src.right_email_masked, src.right_phone_masked, src.right_city, src.right_postcode,
                src.confidence, src.band, src.explanation, src.evidence, COALESCE(src.priority, 0), N'PENDING',
                @run_id, @run_id);

    UPDATE q
       SET q.status = N'SUPERSEDED', q.updated_at = SYSUTCDATETIME()
      FROM review.review_queue AS q
     WHERE q.status = N'PENDING'
       AND NOT EXISTS (SELECT 1 FROM review.review_queue_stage AS s WHERE s.pair_id = q.pair_id);

    TRUNCATE TABLE review.review_queue_stage;

    COMMIT TRANSACTION;

    SELECT status, COUNT(*) AS items FROM review.review_queue GROUP BY status;
END;
GO
