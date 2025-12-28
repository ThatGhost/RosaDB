# RosaDB Project Overview

This document provides a technical overview of the RosaDB project's current architecture and key components.

## 1. Project Goal

RosaDB is an in-development database solution written in C# targeting .NET 10.

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
-   **`Cell`**: A unique logical grouping within a `Database`. A `Cell` can contain multiple `Table` definitions and is a key organizational unit. It is to be analogous to a partition but with metadata attached.
-   **`Table`**: Standard relational concept, containing `Column` definitions.
-   **`Column`**: Defines the name and `DataType` for data within a `Table`.
-   **`Row`**: Represents a single record within a `Table`.

### 3.2. Storage Engine (`RosaDB.Library/StorageEngine`)

The storage mechanism is inspired by log-structured merge-trees (LSM-trees), emphasizing append-only writes with periodic compaction.

#### 3.2.1. Persistence & File Formats
Data persistence is handled by the `LogManager` and split into segments.

-   **Data Segments (`.dat` files)**:
    -   Store actual data records as `Log` entries.
    -   **Serialization**: Custom binary format via `LogSerializer`.
        -   Format: `[Length (4B)][LogId (8B)][IsDeleted (1B)][Date (8B)][TupleDataLength (4B)][TupleData (N bytes)]`.
    -   `TupleData` itself is a binary serialization of a `Row` (via `RowSerializer`).

-   **Index Files (B+Tree)**:
    -   Instead of sparse indexes, a persistent B+Tree implementation (`CSharpTest.Net.BPlusTree` NuGet package) is used for efficient indexing.
    -   These index files (`.idx`) store `LogId`s (keys) mapped to `LogLocation`s (values).
    -   **`LogLocation`**: A `record struct` containing `SegmentNumber` (int) and `Offset` (long) to precisely locate a log entry within its data segment.
    -   **Serialization**: `LogLocation` is serialized using `LogLocationSerializer`.

-   **Environment Files (`_env`)**:
    -   Store metadata for Cells and Databases.
    -   **Serialization**: Length-prefixed JSON via `ByteObjectConverter` (retained for readability/debuggability).

#### 3.2.2. Write Path (`Commit`)
-   Modifications (`Put`/`Delete`) are buffered in an in-memory `_writeAheadLogs` queue.
-   **`Commit` Process**:
    1.  **Condensation**: Logs are condensed via `LogCondenser`. Tombstones (`IsDeleted=true`) are preserved to mask previous data, but intermediate updates for deleted keys within the batch are discarded.
    2.  **Serialization**: Condensed logs are serialized to binary.
    3.  **Persistence**:
        -   Appropriate `.dat` files are identified or created (rollover at 1MB).
        -   Persistent `FileStream`s are opened for the duration of the commit to minimize I/O overhead.
        -   Data is appended to the `.dat` stream.
        -   The B+Tree index is updated with `LogId` and `LogLocation` for each committed log, ensuring efficient lookups.

#### 3.2.3. Read Path
-   **`FindLastestLog`**:
    -   Uses the B+Tree index (`IndexManager.Search`) to directly find the `LogLocation` (segment number and offset) of a specific log entry.
    -   Reads the log directly from the `.dat` file at the specified offset.
-   **`GetAllLogs...`**:
    -   To retrieve the latest state for a cell or table instance, logs are read from the relevant data segments (either from in-memory buffer or disk `.dat` files).
    -   Deduplication logic using a `HashSet` ensures only the first encountered (i.e., latest) version of a Log ID is returned based on `Log.Date`.

### 3.3. Server (`RosaDB.Library/Server`)

-   **Communication**: Implements a `TcpListener` to accept incoming client connections.
-   **Dependency Injection**: Uses `LightInject` for managing dependencies. Key managers (`LogManager`, `IndexManager`, `CellManager`, `DatabaseManager`, `RootManager`) are registered as Scoped per session, and interfaces (e.g., `IIndexManager`) are used for better abstraction.
-   **Client Handling**: Each incoming client connection is handled by a `ClientSession` running in its own asynchronous task.

### 3.4. Querying (`RosaDB.Library/Query`)

-   **Tokenization**: A `QueryTokenizer` class breaks down query strings into tokens.
-   **Mock Queries**: `RosaDB.Library/MoqQueries` contains hardcoded query implementations (e.g., `RandomDeleteLogsQuery`) used for testing and development. These queries now typically return `Result` objects for consistent error handling.

### 3.4.1. Custom Query Syntax

To reflect the unique, cell-based architecture of RosaDB, DML operations (`SELECT`, `INSERT`, `UPDATE`, `DELETE`) use a custom syntax that makes the 'Cell' a first-class citizen.

The general structure is:

```sql
SELECT ...
FROM <CellGroup>.<TableName>
[USING <cell_filter>]
[WHERE <table_data_filter>];
```

#### Components:

1.  **`FROM <CellGroup>.<TableName>`** (Mandatory)
    *   This clause specifies the primary target for the query.
    *   **`<CellGroup>`**: The logical grouping of cells being targeted (e.g., `sales`, `logs`, `users`).
    *   **`<TableName>`**: The name of the table within the cell group.

2.  **`USING <cell_filter>`** (Optional)
    *   This provides a dedicated clause to filter and select a specific cell instance from the `CellGroup`.
    *   The filter typically applies to the cell's indexed properties, such as its name. For example: `USING name = 'q4'`.

3.  **`WHERE <table_data_filter>`** (Optional)
    *   This is the standard SQL `WHERE` clause for filtering the rows of data *within* the selected table.

#### DDL Statements:

The custom syntax extends to Data Definition Language (DDL) operations for defining cell structures and tables.

1.  **`CREATE CELL <CellGroup> (<cell_property_definitions>);`**
    *   This statement defines a new `CellGroup` and specifies the schema for the properties of individual cell instances within that group.
    *   **`<CellGroup>`**: The name of the cell group being created (e.g., `sales`).
    *   **`<cell_property_definitions>`**: A comma-separated list of property names and their data types (e.g., `name TEXT PRIMARY KEY, region TEXT, is_active BOOLEAN`). One property must be designated as `PRIMARY KEY` to uniquely identify cell instances within the group.

    *Example:*
    `CREATE CELL sales (name TEXT PRIMARY KEY, region TEXT, is_active BOOLEAN);`

2.  **`CREATE TABLE <CellGroup>.<TableName> (<column_definitions>);`**
    *   This statement defines a new table schema for an entire `CellGroup`. All cell instances within that group will share this table definition.
    *   **`<CellGroup>.<TableName>`**: Specifies the target `CellGroup` and the name of the new table.
    *   **`<column_definitions>`**: A comma-separated list of column names, their data types, and any constraints (e.g., `id INT PRIMARY KEY, product TEXT, amount INT`).

    *Example:*
    `CREATE TABLE sales.transactions (id INT PRIMARY KEY, product TEXT, amount INT);`

#### Examples:

*   **Select all data from a table within a specific cell:**
    `SELECT * FROM sales.transactions USING name = 'q4';`

*   **Insert data into a specific cell and table:**
    `INSERT INTO sales.transactions USING name = 'q4' (id, amount) VALUES (1, 100);`

This syntax provides a clear and expressive way to interact with RosaDB's partitioned data model.

### 3.5. Error Handling (`RosaDB.Library/Core`)

-   Uses a functional `Result<T>` monad pattern for explicit error handling, avoiding exceptions for control flow. The `Result` and `Result<T>` classes are publicly accessible for broad use.

## 4. Client & Server Projects

-   **`RosaDB.Client`**: A Terminal User Interface (TUI) application (using `Terminal.Gui`) that talks to the server.
-   **`RosaDB.Server`**: A standalone server entry point.