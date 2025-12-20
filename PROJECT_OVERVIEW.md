# RosaDB Project Overview

This document provides an overview of the RosaDB project's current architecture and key components, derived from a recent code analysis.

## 1. Project Goal

RosaDB is an in-development database solution written in C# targeting .NET.

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
-   **`Row`**: Represents a single record within a `Table` (implicitly, as it's not a direct model but data within `Log` entries).
-   **`DataType`**: Supported types include `INT`, `BIGINT`, `VARCHAR`, and `BOOLEAN`.

### 3.2. Storage Engine (`RosaDB.Library/StorageEngine`)

The storage mechanism is inspired by log-structured merge-trees (LSM-trees), emphasizing append-only writes.

-   **Persistence Strategy**: Data modifications are recorded as `Log` entries. These logs are batched and written to disk.
-   **File Organization**: Data for specific `Table` instances within `Cell`s is stored in segmented `.dat` files.
-   **Indexing**: Sparse indexes (`.idx` files) are created alongside data segments to facilitate faster lookups by `Log.Id` and offset.
-   **Serialization**: Objects are serialized to disk using `ByteObjectConverter`, which employs **length-prefixed JSON** (`System.Text.Json`). The object's JSON representation is converted to UTF8 bytes, and its length (4 bytes) is prepended to the byte array.
-   **Concurrency**: Currently, atomicity for certain operations (e.g., `DeleteCell`) is explicitly noted as a pending improvement.

### 3.3. Server (`RosaDB.Library/Server`)

-   **Communication**: Implements a `TcpListener` to accept incoming client connections.
-   **Dependency Injection**: Uses `LightInject` for managing dependencies within server-side components.
-   **Client Handling**: Each incoming client connection is handled by a `ClientSession` running in its own asynchronous task.

### 3.4. Querying (`RosaDB.Library/Query`)

-   **Tokenization**: A `QueryTokenizer` class is responsible for breaking down raw query strings into individual tokens, recognizing parentheses, semicolons, and whitespace as delimiters.

### 3.5. Error Handling (`RosaDB.Library/Core`)

-   The project utilizes a `Result<T>` monad pattern (`Result.cs`) for robust error handling, returning `Success` or `Error` objects instead of relying heavily on exceptions for control flow.

## 4. `RosaDB.Client` Project

-   **Purpose**: Provides a user interface for interacting with the RosaDB server.
-   **Technology**: Built as a Terminal User Interface (TUI) using the `Terminal.Gui` library.
-   **Current Setup**: Notably, the `RosaDB.Client` application currently **instantiates and starts the `RosaDB.Library.Server` directly within its own process** as a background task.

## 5. `RosaDB.Server` Project

-   This project is likely intended as a standalone server application, but its `Program.cs` was not thoroughly analyzed due to the `RosaDB.Client`'s self-contained server initiation. It would typically serve as the primary entry point for a dedicated database server process.

## 6. Key Implementation Details & Future Considerations

-   **JSON Serialization**: While convenient, using JSON for on-disk storage may lead to performance bottlenecks and larger storage footprints compared to more compact binary serialization formats.
-   **Atomicity**: Enhancements for transactional safety and atomicity are needed for data consistency.
-   **Server Decoupling**: For a production-ready system, the server component should be run as a separate process, decoupled from any client application.

This overview should serve as a useful starting point for understanding the RosaDB project.