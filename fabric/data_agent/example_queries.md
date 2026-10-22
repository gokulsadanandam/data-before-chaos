# Example queries (question -> SQL) for the Lakehouse source

**How many customers can we reach on WhatsApp in Bengaluru?**
```sql
SELECT COUNT(DISTINCT master_key) AS customers
FROM gld_customer_current
WHERE consent_whatsapp = 'yes' AND city = 'Bengaluru'
  AND confidence_band IN ('HIGH', 'SINGLE', 'MEDIUM');
```

**Top 10 customers by spend in the last 90 days**
```sql
SELECT c.master_key, c.first_name, c.city, SUM(o.amount) AS spend
FROM gld_order AS o
JOIN gld_customer_current AS c ON c.master_key = o.master_key
WHERE o.order_date >= DATEADD(day, -90, CAST(GETDATE() AS date))
GROUP BY c.master_key, c.first_name, c.city
ORDER BY spend DESC
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;
```

**How much did the DIWALI_26 campaign waste on duplicate or non-consented sends?**
```sql
SELECT SUM(cost) AS wasted_cost, COUNT(*) AS wasted_sends
FROM gld_engagement
WHERE campaign = 'DIWALI_26' AND (duplicate_send = 1 OR sent_without_consent = 1);
```

**Customers who bought online and in store**
```sql
SELECT COUNT(*) AS customers
FROM (
    SELECT master_key FROM gld_order
    WHERE master_key IS NOT NULL
    GROUP BY master_key
    HAVING COUNT(DISTINCT channel) > 1
) AS t;
```

**Email-consented customers who have not opened any email this year**
```sql
SELECT COUNT(*) AS customers
FROM gld_customer_current AS c
WHERE c.consent_email = 'yes'
  AND NOT EXISTS (
      SELECT 1 FROM gld_engagement AS e
      WHERE e.master_key = c.master_key AND e.channel = 'email' AND e.opened = 1
        AND YEAR(e.sent_date) = YEAR(GETDATE())
  );
```
