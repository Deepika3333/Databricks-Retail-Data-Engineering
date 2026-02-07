-- Check duplicate Customer_IDs (example from report)
SELECT *
FROM retail.gold.dim_customer
WHERE Customer_ID IN (
  SELECT Customer_ID
  FROM retail.gold.dim_customer
  GROUP BY Customer_ID
  HAVING COUNT(*) > 1
)
ORDER BY Customer_ID;

-- Check duplicate Transaction_IDs (example from report)
SELECT *
FROM retail.bronze.retail_transactions
WHERE Transaction_ID IN (
  SELECT Transaction_ID
  FROM retail.bronze.retail_transactions
  GROUP BY Transaction_ID
  HAVING COUNT(*) > 1
)
ORDER BY Transaction_ID;
