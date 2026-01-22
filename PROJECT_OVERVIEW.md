# RosaDB Project Overview

This document provides a technical overview of the RosaDB project's current architecture and key components.
It is both intended for new contributors to understand the system and for existing developers to have a reference guide.

## 1. Project Goal

RosaDB is an in-development database solution written in C# targeting .NET 10, designed as a reactive, partitioned datastore ideal for real-time applications.

## 1.1. Key Features

-   **Context-based Architecture**: Data is partitioned into logical groups called "Contexts," allowing for fine-grained organization and querying.
-   **Real-time Subscriptions**: Clients can subscribe to changes (INSERT, UPDATE, DELETE) within a specific context instance via WebSockets, enabling reactive, real-time application development.
-   **Log-Structured Storage**: An append-only storage engine inspired by LSM-trees provides efficient writes and clear data lineage.
-   **Schema Evolution**: Supports adding and removing columns from `ContextGroup` schemas with built-in data migration.

## 1.2. Typical Use Cases

The unique architecture of RosaDB makes it an ideal backend for:
-   **Multi-Tenant SaaS Applications**: Each customer tenant can be represented by a `ContextInstance`, providing strong data isolation while allowing administrators to run analytics across all tenants.
-   **Real-Time IoT Platforms**: Each IoT device can be a `ContextInstance`, allowing for high-volume data ingestion and live monitoring of specific devices via subscriptions.
-   **Collaborative Applications (e.g., Figma, Google Docs)**: Each document or session can be a `ContextInstance`, with the database handling the real-time broadcasting of changes to all subscribed users.
-   **MMO & Online Gaming**: Each game match or player inventory can be a `ContextInstance`, with player actions pushed to subscribed clients in real-time.

## 2. Core Components

The solution is composed of three main projects:
-   **`RosaDB.Library`**: The core database engine and server logic.
-   **`RosaDB.Client`**: A Terminal User Interface (TUI) client application.
-   **`RosaDB.Server`**: A standalone server application.

## 3. `RosaDB.Library` - The Core Database Engine

This project encapsulates all the fundamental logic for the database.

### 3.1. Data Model

The database's logical structure follows a hierarchical model:

-   **`Database`**: The top-level container for a specific database instance.
-   **`ContextGroup`**: A logical category of contexts, defined by a `CREATE CONTEXT` statement, which specifies the schema for context instance properties.
-   **`ContextInstance`**: A specific partition within a `ContextGroup`, created with `INSERT CONTEXT`.
-   **`Table`**: Standard relational concept. Table schemas are defined at the `ContextGroup` level.
-   **`Column`**: Defines the name and `DataType` for data within a `Table`.
-   **`Row`**: Represents a single record within a `Table`.

### 3.2. Storage Engine (`RosaDB.Library/StorageEngine`)

The storage mechanism is inspired by log-structured merge-trees (LSM-trees), emphasizing append-only writes with periodic compaction.

#### 3.2.1. Persistence & File Formats
Data persistence is handled by the `LogManager` and split into segments.

-   **Data Segments (`.dat` files)**: Store actual data records as `Log` entries.
-   **Index Files (B+Tree)**: A persistent B+Tree (`CSharpTest.Net.BPlusTree`) is used for indexing `LogId`s to their `LogLocation` (segment and offset).
-   **Environment Files (`_env`)**: Store metadata for Contexts and Databases using length-prefixed JSON.

#### 3.2.2. Write Path (`Commit`)
-   Modifications are buffered in-memory.
-   **`Commit` Process**:
    1.  Logs are condensed and serialized.
    2.  Data is appended to the current data segment.
    3.  The B+Tree index is updated with the new log locations.
    4.  **Notify Subscribers**: After a successful commit, the `LogManager` notifies a `SubscriptionManager`, which then pushes changes to clients subscribed to the affected context instance.

#### 3.2.3. Read Path & Consistency
-   Reads on a specific context instance are strongly consistent.
-   For cross-context queries (`SELECT` without `USING`), read consistency is on a per-context-read basis. The query does not operate on a single, global snapshot of the entire database, prioritizing availability and performance.

#### 3.2.4. Schema Evolution & Row Format
To support schema evolution (i.e., adding or dropping columns), the on-disk format for serialized rows was updated. Each `ContextInstance` row is now prefixed with an integer indicating the number of columns present when it was written.

-   **On Read**: When `RowSerializer` deserializes data, it reads this column count first. If the current schema has more columns than the persisted data (e.g., after an `ADD COLUMN`), the missing columns are treated as `null`.
-   **On `DROP COLUMN`**: This versioning enables a safe data migration. The `ContextManager` reads each row using its original schema, creates a new row conforming to the new (smaller) schema, and overwrites the old data.

### 3.3. Server (`RosaDB.Library/Server`)

-   **Communication**: Implements a `TcpListener` for standard queries and a WebSocket listener for managing real-time subscriptions.
-   **Dependency Injection**: Uses `LightInject` for managing dependencies.
-   **Client Handling**: Each incoming client connection is handled by a `ClientSession` running in its own asynchronous task.

### 3.4. Querying (`RosaDB.Library/Query`)

#### 3.4.1. Custom Query Syntax

The general structure for DML queries is:
```sql
SELECT ...
FROM <ContextGroup>.<TableName>
[USING <context_filter>]
[WHERE <table_data_filter>];
```

**Components:**
1.  **`FROM <ContextGroup>.<TableName>`** (Mandatory)
    *   Specifies the primary target.
    *   If `USING` is omitted, the query operates across *all* context instances in the group.
2.  **`USING <context_filter>`** (Optional)
    *   Selects a specific context instance based on its indexed properties (e.g., `USING name = 'q4'`).
3.  **`WHERE <table_data_filter>`** (Optional)
    *   Standard SQL `WHERE` clause for filtering data rows within the table(s).

#### 3.4.2. DDL, Context Management & Schema Evolution

The custom syntax extends to DDL for managing the lifecycle of database objects.

-   **`CREATE CONTEXT <ContextGroup> (<props>);`**: Defines a new context group and the schema for its instances.
    *   *Example:* `CREATE CONTEXT sales (name TEXT PRIMARY KEY, region TEXT);`
-   **`INSERT CONTEXT <ContextGroup> (<props>) VALUES (<vals>);`**: Creates a new instance of a context.
    *   *Example:* `INSERT CONTEXT sales (name, region) VALUES ('q4', 'EMEA');`
-   **`UPDATE CONTEXT <ContextGroup> USING <filter> SET ...;`**: Updates properties of a context instance.
-   **`DELETE CONTEXT <ContextGroup> USING <filter>;`**: Deletes a context instance and all its data.
-   **`CREATE TABLE <ContextGroup>.<TableName> (<cols>);`**: Defines a table schema for an entire context group.
    *   *Example:* `CREATE TABLE sales.transactions (id INT PRIMARY KEY, amount INT);`
-   **`ALTER CONTEXT ...`**: Modifies the schema of a `ContextGroup`.
    -   **`ALTER CONTEXT <ContextGroup> ADD COLUMN <colName> <colType>;`**: Adds a new, nullable column to the `ContextGroup` schema. This is a fast, metadata-only operation. Old rows will have a `null` value for this column when read.
    -   **`ALTER CONTEXT <ContextGroup> DROP COLUMN <colName>;`**: Removes a column from the `ContextGroup` schema. **This is a slow, blocking operation** as it requires a full data migration. Every row in every instance of the `ContextGroup` is rewritten to conform to the new schema.
-   **`ALTER TABLE...`**: Syntax for altering table schemas is planned but not yet implemented.

#### 3.4.3. Metadata & Discoverability

The following commands are available for discovering the database schema:
-   **`SHOW CONTEXT GROUPS;`**: Lists all defined context groups.
-   **`SHOW TABLES IN <ContextGroup>;`**: Lists all table schemas defined for a specific context group.
-   Queryable metadata tables (e.g., `system.contexts`) are planned for more advanced introspection.

#### 3.4.4. JOINs

-   **Scope**: `JOIN`s are supported but are restricted to tables within the *same* `ContextGroup` and (if specified) the *same* `ContextInstance`. Cross-group joins are not supported.
-   *Example:* `SELECT t.*, r.region_name FROM sales.transactions AS t JOIN sales.regions AS r ON t.region_id = r.id;`

### 3.5. Error Handling (`RosaDB.Library/Core`)

-   Uses a functional `Result<T>` monad pattern for explicit error handling, leveraging a chain of `.Then()` and `.ThenAsync()` extension methods.
-   For the final operation in a chain that performs a side effect but returns no value, a `.Finally()` method is used to terminate the chain and return a non-generic `Result`.

### 3.6. Authentication and Authorization

-   **Current Scope**: The core database engine does not have a built-in user or role system.
-   **Implementation Strategy**: Authorization is delegated to the implementor. The server will provide a WebSocket endpoint for connection events. An external service can listen to these events and determine a connection's permissions by issuing `GRANT`/`REVOKE` commands (planned for a future release) on its behalf.

## 4. Client & Server Projects

-   **`RosaDB.Client`**: A Terminal User Interface (TUI) application. The primary focus is on making this as powerful as possible, as standard SQL tools are not compatible with the custom syntax.
-   **`RosaDB.Server`**: A standalone server entry point.