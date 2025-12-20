# RosaDB Project Overview

This document provides a technical overview of the RosaDB project's current architecture and key components.

## 1. Project Goal

RosaDB is an in-development database solution written in C# targeting .NET 10.

## 2. Core Components

The solution is composed of three main projects:
-   **`RosaDB.Library`**: The core database engine and server logic.
-   **`RosaDB.Client`**: A Terminal User Interface (TUI) client application.
-   **`RosaDB.Server`**: (Intended) a standalone server application.

## 3. `RosaDB.Library` - The Core Database Engine

This project encapsulates all the fundamental logic for the database.

### 3.1. Data Model

The database's logical structure follows a hierarchical model:

-   **`Database`**: The top-level container for a specific database instance.
-   **`Cell`**: A unique logical grouping within a `Database`. A `Cell` can contain multiple `Table` definitions and is a key organizational unit. It appears to be analogous to a schema or a partition.
-   **`Table`**: Standard relational concept, containing `Column` definitions.
-   **`Column`**: Defines the name and `DataType` for data within a `Table`.
-   **`Row`**: Represents a single record within a `Table`.
-   **`DataType`**: Supported types include `INT`, `BIGINT`, `VARCHAR`, and `BOOLEAN`.

### 3.2. Storage Engine (`RosaDB.Library/StorageEngine`)

The storage mechanism is inspired by log-structured merge-trees (LSM-trees), emphasizing append-only writes with periodic compaction.

#### 3.2.1. Persistence & File Formats
Data persistence is handled by the `LogManager` and split into segments.

-   **Data Segments (`.dat` files)**:
    -   Store actual data records as `Log` entries.
    -   **Serialization**: Custom binary format via `LogSerializer`.
        -   Format: `[Length (4B)][LogId (8B)][IsDeleted (1B)][Date (8B)][TupleDataLength (4B)][TupleData (N bytes)]`.
    -   `TupleData` itself is a binary serialization of a `Row` (via `RowSerializer`).

-   **Index Segments (`.idx` files)**:
    -   Store sparse indexes pointing to offsets in the corresponding `.dat` file.
    -   **Header**: `IndexHeader` written at the start of the file.
        -   Serialization: `IndexSerializer`. Format: `[Version (4B)][CellName (Str)][TableName (Str)][InstanceHash (Str)][SegmentNumber (4B)]`.
    -   **Entries**: `SparseIndexEntry` written periodically (every 100 records).
        -   Serialization: `IndexSerializer`. Format (20 bytes fixed): `[Version (4B)][Key/LogId (8B)][Offset (8B)]`.

-   **Environment Files (`_env`)**:
    -   Store metadata for Cells and Databases.
    -   **Serialization**: Length-prefixed JSON via `ByteObjectConverter` (retained for readability/debuggability).

#### 3.2.2. Write Path (`Commit`)
-   Modifications (`Put`/`Delete`) are buffered in an in-memory `_writeAheadLogs` queue.
-   **`Commit` Process**:
    1.  **Condensation**: Logs are condensed via `LogCondenser`. Tombstones (`IsDeleted=true`) are preserved to mask previous data, but intermediate updates for deleted keys within the batch are discarded.
    2.  **Serialization**: Condensed logs are serialized to binary.
    3.  **Persistence**:
        -   Appropriate `.dat` and `.idx` files are identified or created (rollover at 1MB).
        -   Persistent `FileStream`s are opened for the duration of the commit to minimize I/O overhead.
        -   Data is appended to the `.dat` stream.
        -   Sparse index entries are appended to the `.idx` stream every 100 records.

#### 3.2.3. Read Path
-   **`GetAllLogs...`**:
    -   To retrieve the latest state, logs are read in **reverse chronological order**:
        1.  In-memory buffer is iterated in reverse.
        2.  Disk segments are iterated from newest (highest segment number) to oldest.
        3.  Logs within each segment are read fully and then reversed.
    -   Deduplication logic ensures only the first encountered (i.e., latest) version of a Log ID is returned.

### 3.3. Server (`RosaDB.Library/Server`)

-   **Communication**: Implements a `TcpListener` to accept incoming client connections.
-   **Dependency Injection**: Uses `LightInject` for managing dependencies (`LogManager` is Scoped per session).
-   **Client Handling**: Each incoming client connection is handled by a `ClientSession` running in its own asynchronous task.

### 3.4. Querying (`RosaDB.Library/Query`)

-   **Tokenization**: A `QueryTokenizer` class breaks down query strings into tokens.
-   **Mock Queries**: `RosaDB.Library/MoqQueries` contains hardcoded query implementations (e.g., `RandomDeleteLogsQuery`) used for testing and development.

### 3.5. Error Handling (`RosaDB.Library/Core`)

-   Uses a functional `Result<T>` monad pattern for explicit error handling, avoiding exceptions for control flow.

## 4. Client & Server Projects

-   **`RosaDB.Client`**: A TUI application (using `Terminal.Gui`) that currently embeds and runs the `RosaDB.Library.Server` in a background task.
-   **`RosaDB.Server`**: A standalone server entry point.

## 5. Technical Constraints & Todos

-   **Atomicity**: File operations are not yet fully atomic (no WAL or two-phase commit for file system operations).
-   **Server Decoupling**: The Client currently embeds the Server, which should be decoupled for production.
