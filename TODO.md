# RosaDB Todo List

## High Priority

- [ ] **Primairy key indexes**: Implement the b-tree logic to facilitate primairy keys (and other indexes) on tuples.
- [ ] **Save data on cells**: Cells can have meta data collumns attached to them. Currently only indexes are supported

## Medium Priority

- [ ] **Implement JOIN**: Get a query ready that will do a JOIN
- [ ] **Query Engine Improvements**: Expand `QueryTokenizer` and implement a proper parser and execution engine for SQL-like queries (beyond simple tokenization).
- [ ] **Data Validation**: Enhance `DataValidator` to support more complex constraints (e.g., NOT NULL, UNIQUE).
- [ ] **Start Subscription Managment**: Begin the implementation for websocket subscriptions to cells.

## Low Priority

- [ ] **Documentation**: Expand code comments and add architectural documentation (like diagrams).
- [ ] **Unit Tests**: Increase test coverage for `LogManager` and `CellManager`, especially for edge cases and failure scenarios.
- [ ] **TUI Enhancements**: Improve the `RosaDB.Client` TUI with better navigation and query result visualization.
- [ ] **Loggin**: Add logging to the server. Make this available through a websocket as a built in cell