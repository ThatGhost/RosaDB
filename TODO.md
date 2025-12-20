# RosaDB Todo List

## High Priority

- [ ] **Atomicity & Transactions**: `DatabaseManager.DeleteCell` and other operations lack transactional safety. Implement a mechanism to ensure operations are atomic (all or nothing).
- [ ] **Index Serialization**: Convert `IndexHeader` and `SparseIndexEntry` serialization to binary (currently JSON via `ByteObjectConverter`) to match the new efficient `Log` format.

## Medium Priority

- [ ] **Server Decoupling**: Refactor `RosaDB.Client` to not embed `RosaDB.Library.Server`. The server should run as a standalone process (`RosaDB.Server`), and the client should connect to it via TCP.
- [ ] **Query Engine Improvements**: Expand `QueryTokenizer` and implement a proper parser and execution engine for SQL-like queries (beyond simple tokenization).
- [ ] **Data Validation**: Enhance `DataValidator` to support more complex constraints (e.g., NOT NULL, UNIQUE).

## Low Priority

- [ ] **Documentation**: Expand code comments and add architectural documentation (like diagrams).
- [ ] **Unit Tests**: Increase test coverage for `LogManager` and `CellManager`, especially for edge cases and failure scenarios.
- [ ] **TUI Enhancements**: Improve the `RosaDB.Client` TUI with better navigation and query result visualization.

## Completed

- [x] **Log Serialization**: Switched `Log` object serialization from JSON to custom binary format for improved performance and storage efficiency.

