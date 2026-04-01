CREATE TABLE customer_insights (
  cust_id STRING(36) NOT NULL,
  acct_no STRING(36) NOT NULL,
  phone_number STRING(20) NOT NULL,
  insight_category STRING(50) NOT NULL,
  insight_name STRING(100) NOT NULL,
  insight_values STRING(MAX),
  updated_by STRING(100),
  updated_at TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY(cust_id, acct_no, phone_number, insight_category, insight_name);

CREATE TABLE customer_insights_phone (
  cust_id STRING(36) NOT NULL,
  acct_no STRING(36) NOT NULL,
  phone_number STRING(20) NOT NULL,
  insight_category STRING(50) NOT NULL,
  insight_name STRING(100) NOT NULL,
  insight_values STRING(MAX),
  updated_by STRING(100),
  updated_at TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY(phone_number, insight_category, insight_name);
