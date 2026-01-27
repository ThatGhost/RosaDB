# RosaDB Development Plan

This document outlines a sequential development plan for RosaDB.

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

## ~~Phase 3: Implement SELECT and Insert queries~~
1. ~~**Implement Basic SELECT Queries:**~~
    ~~*   Extend `QueryTokenizer` and `QueryPlanner` to support basic `SELECT ... FROM ... WHERE ...` syntax.~~
    ~~*   Implement a `SelectOperator` that can retrieve rows from a table based on simple `WHERE` conditions (e.g., equality checks).~~
2. ~~**Integrate `IndexManager` into Query Path:**~~
    ~~*   Update the `QueryPlanner` to identify and utilize B-Tree indexes for accelerating queries with `WHERE` clauses on indexed columns, performing B-Tree searches instead of full table scans.~~
3. ~~**Implement Basic INSERT Queries:**~~
    *   ~~Extend `QueryTokenizer` and `QueryPlanner` to support basic `INSERT INTO ... VALUES ...` syntax.~~
    *   ~~Implement an `InsertOperator` that can add new rows to a table, ensuring data is correctly serialized and stored using the existing `LogManager`.~~
4. ~~**Implement USING in SELECT Queries:**~~
   ~~*   Add the `USING` keywork to select queries to be able to search on Context instances.~~
    ~~*   Add the `USING` keywork to select queries to be able to search on Context instances with clause test.~~
    ~~*   Use indexes for the indexed properties of the context~~
5. ~~**Add Greater and Lesser than operations to `SELECT`**~~

## ~~Phase 4: Feature Development~~

1. ~~**Implement `UNIQUE` Constraint:** Leverage the B-Tree indexing to enforce `UNIQUE` constraints during `INSERT` operations by checking for existing values in the index.~~
2. ~~**Implement Real-time Subscriptions (WebSockets):**~~
    *   ~~Create and register a `SubscriptionManager` service using `LightInject`.~~
    *   ~~Enhance `Websockets` to process a `SUBSCRIBE` command, registering client interests with the `SubscriptionManager`.~~
    *   ~~Modify `LogManager.Commit` to notify the `SubscriptionManager` of data changes, allowing it to push updates to subscribed WebSocket clients (leveraging the existing `/ws` endpoint in `Program.cs`).~~
3. ~~**Context Metadata:**~~
    ~~*   Add a `Dictionary<string, object> Metadata` property to the `Context` model (`Context.cs`).~~
    ~~*   Update `ContextEnvironment.cs` to include this new metadata, relying on `ByteObjectConverter`'s JSON serialization.~~
    ~~*   Implement an `ALTER CONTEXT` query within the `QueryPlanner` to enable adding, updating, or removing key-value pairs from context metadata.~~ (Only added ADD and DROP Column for now)
4. ~~**Implement Transactions:**~~
    ~~*   Design a transaction model that allows grouping multiple `INSERT`, `UPDATE`, and `DELETE` operations into a single atomic unit.~~
    ~~*   Implement `BEGIN TRANSACTION`, `COMMIT`, and `ROLLBACK` commands in the `QueryPlanner`.~~
    ~~*   Modify the `LogManager` to support transactional logging, ensuring that changes are only persisted upon `COMMIT` and can be reverted on `ROLLBACK`.~~
5. ~~**Multiline Queries:**~~
    ~~*   Introduce the ability to send multiline queries. this should be handled in the `QueryTokenizer` And the `QueryPlanner`.~~

## ~~Phase 5: Developer Experience and Server Enhancements~~

1. ~~**Integrate Logging Framework:** Implement a robust logging framework (e.g., Serilog, NLog) across `RosaDB.Server` and `RosaDB.Library`, replacing existing `Console.WriteLine` calls with structured logging.~~
   *    ~~Add this to a special context instance that users can `SUBSCRIBE` to using the websockets.~~
   *    ~~Add a session id to all sessions and make the logs identifiable by session id.~~ 
2. ~~**TUI Enhancements:** Improve the `ContentView` in the `RosaDB.Client` to enhance the readability and formatting of query results.~~
3. ~~**Saving of queries in the TUI:** Add the ability to save queries and delete them. This should be persisted throughout sessions. This will replace the current `DefaultQueryView.cs`~~
4. ~~**Better seeder:** Add better seeding data. with lots of context instances and maybe tables. Real-ish data.~~

## ~~Phase 5.5: Change `CONTEXT` to `CONTEXT`~~

## Phase 6: Rigorous Testing and further Feature Development
1. **Unit Testing:** Testing should be the applied to every line and every type of query. Good and bad paths.
   * ~~ContextManager~~
   * DatabaseManager
   * FolderManager
   * IndexManager
   * LogManager
   * SelectQuery
   * DeleteQuery
   * InsertQuery
   * UpdateQuery
   * AlterQuery
   * DropQuery
   * UseQuery
   * Transactions
2. **Integration Tests:** Add integration testing to Every type of query and see that the effects are correct.
3. **Add `DELETE` Query:** Add the ability to delete rows or contexts.
    *   ~~Take into account transactions~~
4. **Altering of `Tables`:** Add the ability to alter tables and update its rows using the `ALTER TABLE` query.
5. **Add `COMPACT`:** This query should start compacting log files into higher level ones.
6. **Add `UPDATE` Query:** Add the ability to update rows or contexts.
    *   Take into account transactions

## Phase 7: Optimization
1. **Performance Profiling:** Use profiling tools to identify bottlenecks in query execution and data storage.
2. **Optimize Data Structures:** Refine data structures and algorithms based on profiling results to enhance performance.
3. ~~**Index Optimization:** Analyze and optimize B-Tree index structures for faster lookups and reduced storage overhead.~~
4. ~~**Caching Mechanisms:** Implement caching strategies for frequently accessed data to reduce disk I/O and improve response times.~~
5. ~~**Allocation Optimization:** Review and optimize memory allocation patterns to minimize fragmentation and improve garbage collection efficiency.~~
6. **Websockets dedicated thread:** Websockets and its callbacks should run on its own separate thread to not interfere with regular queries.

## Phase 8: User/Settings management and Connection strings

## Phase 9: Missing features
1.  **`ALTER TABLE` & `CONTEXT` Modify columns:** You can only `ADD` or `DROP` columns right now. add the modify column feature.
2.  **Default Context:** When a context is not present in the query. Replace the context with `default`. This will be used as a context with one instance that can function as a regular database
3.  **Foreign keys:** Implement foreign keys. they can link tables of the same context or tables and contexts.
     * Example: `TABLE` Users in the default connects to the accounts `CONTEXT`.
4. **Rework `DELETE` Query**: Query now get all the instances and uses a crude method. 
     * If the indexes are present only use the indexes
     * Stream the Context instances
     * Smarter lookups if possible
     * Make it work for context instances as well.
5. **Rework the `LogManager` ... again**: The `LogManager` should be split in reading and writing. And rework the instance hash out of the parameters
