# RosaDB Development Plan

This document outlines a sequential development plan for RosaDB, based on a detailed review of the codebase.

## ~~Phase 1: Foundations & Parser Enhancement~~

1.  ~~**Create Test Project:** Create a new xUnit or NUnit test project within the solution to house unit and integration tests for `RosaDB.Library`. This is crucial before making major changes.~~
2.  **Test Core Storage:** ~~Add integration tests for the `LogManager`'s `Commit` and `GetAllLogs...` methods. Add unit tests for `RowSerializer`, `LogSerializer`, and `IndexSerializer` to ensure data integrity and proper serialization.~~
3.  ~~**Refactor `QueryTokenizer`:** Improve `QueryTokenizer.cs` to handle string literals (e.g., 'hello world'), quoted identifiers, and a wider range of special characters more robustly without splitting them incorrectly.~~
4.  ~~**Enhance `TokensToColumnsParser` for Constraints:**~~
    *   ~~Modify `TokensToColumnsParser.TokensToColumn` to parse `PRIMARY KEY` and `INDEX` keywords from `CREATE TABLE` statements.~~
    *   ~~Set the existing `IsPrimaryKey` and `IsIndex` flags on the `Column` model (`Column.cs`).~~
    *   ~~Add an `IsNullable` property to `Column.cs` and implement parsing for `NOT NULL` constraints.~~

## ~~Phase 2: B-Tree Indexing Implementation~~

1.  ~~**B-Tree Data Structures:** Create the necessary classes for the B-Tree (e.g., `BTreeNode`, `BTreeLeaf`, `BTreeInternalNode`), designed for efficient serialization to disk.~~
2.  ~~**B-Tree Serializer:** Implement a `BTreeSerializer` to manage the conversion of B-Tree nodes to and from fixed-size byte arrays (pages) for persistent storage.~~
3.  ~~**Core B-Tree Logic:** Develop the fundamental B-Tree operations: `Insert`, `Search`, and `Delete`. These will operate on the in-memory representation of nodes, using the serializer for disk interaction.~~
4.  ~~**Create `IndexManager`:** Introduce a new `IndexManager` class within the `StorageEngine` to handle the creation, opening, and management of B-Tree-based index files. This manager will be analogous to the `LogManager`.~~
5.  ~~**Integrate `IndexManager` into Write Path:**~~
    ~~*   Inject the `IndexManager` into the `LogManager`.~~
    ~~*   Modify `LogManager.Commit` to update the relevant B-Tree indexes for indexed columns after persisting data logs.~~

## ~~Phase 2.5: Implement SELECT and Insert queries~~
1. ~~**Implement Basic SELECT Queries:**~~
    ~~*   Extend `QueryTokenizer` and `QueryPlanner` to support basic `SELECT ... FROM ... WHERE ...` syntax.~~
    ~~*   Implement a `SelectOperator` that can retrieve rows from a table based on simple `WHERE` conditions (e.g., equality checks).~~
2. ~~**Integrate `IndexManager` into Query Path:**~~
    ~~*   Update the `QueryPlanner` to identify and utilize B-Tree indexes for accelerating queries with `WHERE` clauses on indexed columns, performing B-Tree searches instead of full table scans.~~
3. ~~**Implement Basic INSERT Queries:**~~
    *   ~~Extend `QueryTokenizer` and `QueryPlanner` to support basic `INSERT INTO ... VALUES ...` syntax.~~
    *   ~~Implement an `InsertOperator` that can add new rows to a table, ensuring data is correctly serialized and stored using the existing `LogManager`.~~
4. ~~**Implement USING in SELECT Queries:**~~
   ~~*   Add the `USING` keywork to select queries to be able to search on Cell instances.~~
    ~~*   Add the `USING` keywork to select queries to be able to search on Cell instances with clause test.~~
    ~~*   Use indexes for the indexed properties of the cell~~
5. ~~**Add Greater and Lesser than operations to `SELECT`**~~

## Phase 3: Feature Development

1. **Implement `UNIQUE` Constraint:** Leverage the B-Tree indexing to enforce `UNIQUE` constraints during `INSERT` and `UPDATE` operations by checking for existing values in the index.
2. **Implement Real-time Subscriptions (WebSockets):**
    *   Create and register a `SubscriptionManager` service using `LightInject`.
    *   Enhance `ClientSession` to process a `SUBSCRIBE` command, registering client interests with the `SubscriptionManager`.
    *   Modify `LogManager.Commit` to notify the `SubscriptionManager` of data changes, allowing it to push updates to subscribed WebSocket clients (leveraging the existing `/ws` endpoint in `Program.cs`).
3. **Cell Metadata:**
    ~~*   Add a `Dictionary<string, object> Metadata` property to the `Cell` model (`Cell.cs`).~~
    ~~*   Update `CellEnvironment.cs` to include this new metadata, relying on `ByteObjectConverter`'s JSON serialization.~~
    *   Implement an `ALTER CELL` query within the `QueryPlanner` to enable adding, updating, or removing key-value pairs from cell metadata.

## Phase 4: Developer Experience and Server Enhancements

1. **Integrate Logging Framework:** Implement a robust logging framework (e.g., Serilog, NLog) across `RosaDB.Server` and `RosaDB.Library`, replacing existing `Console.WriteLine` calls with structured logging.
~~2. **TUI Enhancements:** Improve the `ContentView` in the `RosaDB.Client` to enhance the readability and formatting of query results.~~

## Phase 5: Start research about replication and scaling

1. **Research Replication Strategies:** Investigate various replication methods (e.g., master-slave, multi-master) suitable for RosaDB's architecture. Document findings and propose a replication model.
2. **Research Scaling Techniques:** Explore scaling strategies, including sharding and partitioning

## Phase 6: Optimization
1. **Performance Profiling:** Use profiling tools to identify bottlenecks in query execution and data storage.
2. **Optimize Data Structures:** Refine data structures and algorithms based on profiling results to enhance performance.
3. ~~**Index Optimization:** Analyze and optimize B-Tree index structures for faster lookups and reduced storage overhead.~~
4. ~~**Caching Mechanisms:** Implement caching strategies for frequently accessed data to reduce disk I/O and improve response times.~~
5. ~~**Allocation Optimization:** Review and optimize memory allocation patterns to minimize fragmentation and improve garbage collection efficiency.~~