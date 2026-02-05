# Spanner Graph Demo: Financial Connections

This demo showcases how you can use the Spanner MCP server to perform [Spanner Graph](https://docs.cloud.google.com/spanner/docs/graph/overview) queries to identify relationships between people based on financial transactions. With the Spanner MCP integration, you're able to use the LLM to generate complex graph queries without having to remember the nuances of crafting Spanner graph queries yourself.

## Prerequisites

1.  **Confirm Spanner Details:** Please verify the following details for your Spanner setup:
    *   Project ID: `span-cloud-testing`
    *   Instance ID: `graph-demo`
    *   Database ID: `demo-user-guide`
    Let me know if you need to use different values.

2.  **Instance Edition:** Spanner Graph features require the instance to be on the **ENTERPRISE** or **ENTERPRISE_PLUS** edition. Please ensure the instance `{instance-id}` is configured accordingly.

**Note:** The following DDL and DML statements are intended to be executed using the Spanner MCP tools (e.g., `create_database`, `update_database_schema`, `execute_sql`).

## 1. Database Schema (DDL)

```sql
CREATE TABLE Account (
  id INT64 NOT NULL,
  create_time TIMESTAMP,
  is_blocked BOOL,
  nick_name STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE Person (
  id INT64 NOT NULL,
  name STRING(MAX),
  birthday TIMESTAMP,
  country STRING(MAX),
  city STRING(MAX),
) PRIMARY KEY(id);

CREATE TABLE AccountTransferAccount (
  id INT64 NOT NULL,
  to_id INT64 NOT NULL,
  amount FLOAT64,
  create_time TIMESTAMP NOT NULL,
  order_number STRING(MAX),
  FOREIGN KEY(to_id) REFERENCES Account(id),
) PRIMARY KEY(id, to_id, create_time),
  INTERLEAVE IN PARENT Account ON DELETE CASCADE;

CREATE TABLE PersonOwnAccount (
  id INT64 NOT NULL,
  account_id INT64 NOT NULL,
  create_time TIMESTAMP,
  FOREIGN KEY(account_id) REFERENCES Account(id),
) PRIMARY KEY(id, account_id),
  INTERLEAVE IN PARENT Person ON DELETE CASCADE;

CREATE PROPERTY GRAPH FinGraph
  NODE TABLES(
    Account
      KEY(id)
      LABEL Account PROPERTIES(
        create_time,
        id,
        is_blocked,
        nick_name),

    Person
      KEY(id)
      LABEL Person PROPERTIES(
        birthday,
        city,
        country,
        id,
        name)
  )
  EDGE TABLES(
    AccountTransferAccount
      KEY(id, to_id, create_time)
      SOURCE KEY(id) REFERENCES Account(id)
      DESTINATION KEY(to_id) REFERENCES Account(id)
      LABEL Transfers PROPERTIES(
        amount,
        create_time,
        id,
        order_number,
        to_id),

    PersonOwnAccount
      KEY(id, account_id)
      SOURCE KEY(id) REFERENCES Person(id)
      DESTINATION KEY(account_id) REFERENCES Account(id)
      LABEL Owns PROPERTIES(
        account_id,
        create_time,
        id)
  );
```

**Tip:** You can also apply this entire schema when creating the database by passing the DDL statements as a list to the `extraStatements` parameter in the `create_database` tool call.

## 2. Populate Data (DML)

**Note:** To avoid potential transaction timeouts, it is recommended to execute the INSERT statements for each table in a separate transaction. Commit the transaction after each table's data is inserted.

**a. Insert Persons:**

```sql
INSERT INTO Person (id, name, birthday, country, city) VALUES
(1, 'Alex', '1990-01-01T00:00:00Z', 'USA', 'New York'),
(2, 'Lee', '1985-05-15T00:00:00Z', 'USA', 'San Francisco'),
(3, 'Dana', '1995-08-20T00:00:00Z', 'Canada', 'Vancouver'),
(4, 'Maria', '1992-04-15T00:00:00Z', 'USA', 'Chicago'),
(5, 'David', '1988-11-20T00:00:00Z', 'Canada', 'Toronto');
```

**b. Insert Accounts:**

```sql
INSERT INTO Account (id, create_time, is_blocked, nick_name) VALUES
(7, '2020-01-10T14:22:20.222Z', false, 'Vacation Fund'),
(16, '2020-01-28T01:55:09.206Z', true, 'Vacation Fund'),
(20, '2020-02-18T13:44:20.655Z', false, 'Rainy Day Fund'),
(25, '2023-01-15T10:00:00Z', false, 'Savings'),
(30, '2023-02-20T11:00:00Z', false, 'Checking');
```

**c. Link Persons to Accounts (PersonOwnAccount):**

```sql
INSERT INTO PersonOwnAccount (id, account_id, create_time) VALUES
(1, 7, '2020-01-10T14:22:20.222Z'),
(2, 16, '2020-01-28T01:55:09.206Z'),
(3, 20, '2020-02-18T13:44:20.655Z'),
(4, 25, '2023-01-15T10:00:00Z'),
(5, 30, '2023-02-20T11:00:00Z');
```

**d. Add Transfers (AccountTransferAccount):**

```sql
INSERT INTO AccountTransferAccount (id, to_id, amount, create_time, order_number) VALUES
(7, 16, 300, '2024-02-28T10:00:00Z', 'ORD123'),
(7, 16, 100, '2024-02-29T11:00:00Z', 'ORD124'),
(16, 20, 300, '2024-02-29T12:00:00Z', 'ORD125'),
(20, 7, 500, '2024-02-29T13:00:00Z', 'ORD126'),
(20, 16, 200, '2024-02-29T14:00:00Z', 'ORD127'),
(7, 25, 150.75, '2024-03-01T12:00:00Z', 'ORD456'),
(25, 30, 200.00, '2024-03-02T14:30:00Z', 'ORD457'),
(30, 16, 50.25, '2024-03-03T16:00:00Z', 'ORD458');
```

## 3. Sample Prompts for GQL Query Generation

Use the Gemini CLI to generate and run GQL queries against the `FinGraph` property graph. Here are some examples:

*   "Find all people who are 1 hop away from each other based on account transfers."
*   "Show me pairs of people who have a 1-hop financial connection."
*   "Identify people who are connected through a maximum of 2 hops of account transfers."
*   "Find indirect financial connections (2 hops) between people."
*   "Who transferred money to Lee?"
*   "What are the financial connections for Alex?"

Example interaction with Gemini CLI:

```
>> Find all people who are 1 hop away from each other based on account transfers.
```
