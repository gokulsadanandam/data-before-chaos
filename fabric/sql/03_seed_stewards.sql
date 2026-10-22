/* Register the people allowed to decide. Replace with your team's Entra UPNs. */
MERGE review.steward AS tgt
USING (VALUES
    (N'steward1@contoso.com', N'Data Steward 1', N'steward'),
    (N'steward2@contoso.com', N'Data Steward 2', N'steward'),
    (N'lead@contoso.com',     N'Data Governance Lead', N'lead')
) AS src (upn, display_name, role)
    ON tgt.upn = src.upn
WHEN MATCHED THEN UPDATE SET tgt.display_name = src.display_name, tgt.role = src.role, tgt.is_active = 1
WHEN NOT MATCHED THEN INSERT (upn, display_name, role) VALUES (src.upn, src.display_name, src.role);
GO
