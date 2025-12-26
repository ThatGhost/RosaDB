# RosaDB Development Plan

This document outlines a sequential development plan for RosaDB, based on a detailed review of the codebase.

## Phase 1: Foundations & Parser Enhancement

1.  ~~**Create Test Project:** Create a new xUnit or NUnit test project within the solution to house unit and integration tests for `RosaDB.Library`. This is crucial before making major changes.~~
2.  **Test Core Storage:** Add integration tests for the `LogManager`'s `Commit` and `GetAllLogs...` methods. Add unit tests for `RowSerializer`, `LogSerializer`, and `IndexSerializer` to ensure data integrity and proper serialization.
3.  **Refactor `QueryTokenizer`:** Improve `QueryTokenizer.cs` to handle string literals (e.g., 'hello world'), quoted identifiers, and a wider range of special characters more robustly without splitting them incorrectly.
4.  **Enhance `TokensToColumnsParser` for Constraints:**
    *   Modify `TokensToColumnsParser.TokensToColumn` to parse `PRIMARY KEY` and `INDEX` keywords from `CREATE TABLE` statements.
    *   Set the existing `IsPrimaryKey` and `IsIndex` flags on the `Column` model (`Column.cs`).
    *   Add an `IsNullable` property to `Column.cs` and implement parsing for `NOT NULL` constraints.

## Phase 2: B-Tree Indexing Implementation

5.  **B-Tree Data Structures:** Create the necessary classes for the B-Tree (e.g., `BTreeNode`, `BTreeLeaf`, `BTreeInternalNode`), designed for efficient serialization to disk.
6.  **B-Tree Serializer:** Implement a `BTreeSerializer` to manage the conversion of B-Tree nodes to and from fixed-size byte arrays (pages) for persistent storage.
7.  **Core B-Tree Logic:** Develop the fundamental B-Tree operations: `Insert`, `Search`, and `Delete`. These will operate on the in-memory representation of nodes, using the serializer for disk interaction.
8.  **Create `IndexManager`:** Introduce a new `IndexManager` class within the `StorageEngine` to handle the creation, opening, and management of B-Tree-based index files. This manager will be analogous to the `LogManager`.
9.  **Integrate `IndexManager` into Write Path:**
    *   Inject the `IndexManager` into the `LogManager`.
    *   Modify `LogManager.Commit` to update the relevant B-Tree indexes for indexed columns after persisting data logs.
10. **Integrate `IndexManager` into Query Path:**
    *   Update the `QueryPlanner` to identify and utilize B-Tree indexes for accelerating queries with `WHERE` clauses on indexed columns, performing B-Tree searches instead of full table scans.

## Phase 3: Feature Development

11. **Implement `UNIQUE` Constraint:** Leverage the newly implemented B-Tree indexing to enforce `UNIQUE` constraints during `INSERT` and `UPDATE` operations by checking for existing values in the index.
12. **Implement `JOIN` Queries:**
    *   Extend `QueryTokenizer` and `QueryPlanner` to support `INNER JOIN ... ON` syntax.
    *   Develop a `JoinOperator` that can merge data from two tables based on specified join conditions. A nested loop join can be a starting point.
13. **Implement Real-time Subscriptions (WebSockets):**
    *   Create and register a `SubscriptionManager` service using `LightInject`.
    *   Enhance `ClientSession` to process a `SUBSCRIBE` command, registering client interests with the `SubscriptionManager`.
    *   Modify `LogManager.Commit` to notify the `SubscriptionManager` of data changes, allowing it to push updates to subscribed WebSocket clients (leveraging the existing `/ws` endpoint in `Program.cs`).
14. **Cell Metadata:**
    *   Add a `Dictionary<string, object> Metadata` property to the `Cell` model (`Cell.cs`).
    *   Update `CellEnvironment.cs` to include this new metadata, relying on `ByteObjectConverter`'s JSON serialization.
    *   Implement an `ALTER CELL` query within the `QueryPlanner` to enable adding, updating, or removing key-value pairs from cell metadata.

## Phase 4: Developer Experience and Server Enhancements

15. **Integrate Logging Framework:** Implement a robust logging framework (e.g., Serilog, NLog) across `RosaDB.Server` and `RosaDB.Library`, replacing existing `Console.WriteLine` calls with structured logging.
16. **TUI Enhancements:** Improve the `ContentView` in the `RosaDB.Client` to enhance the readability and formatting of query results.

## Phase 5: Start research about replication and scaling