# RosaDB

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/ThatGhost/RosaDB)

**RosaDB is a reactive, partitioned datastore written in C# for modern, real-time applications.**

It combines a unique, context-based architecture with a powerful real-time subscription model, making it an ideal backend for multi-tenant SaaS applications, IoT platforms, collaborative tools, and online games.

## Key Features

-   **Context-based Architecture**: Data is partitioned into logical groups called "Contexts," allowing for fine-grained organization, data isolation, and powerful querying capabilities.
-   **Real-time Subscriptions**: Clients can subscribe to data changes (`INSERT`, `UPDATE`, `DELETE`) within a specific context instance via WebSockets, enabling reactive application development without a separate message bus.
-   **Log-Structured Storage**: An append-only storage engine provides efficient writes and clear data lineage.
-   **Custom, Expressive SQL Dialect**: A query language designed from the ground up to make working with contexts and subscriptions intuitive and powerful.

## Getting Started

### Prerequisites

-   .NET 10 SDK

### Building the Project

From the root of the repository, build the entire solution:

```bash
dotnet build RosaDB.sln
```

### Running the Server

Run the server project. It will start a TCP listener for queries and a WebSocket listener for subscriptions.

```bash
dotnet run --project RosaDB.Server/RosaDB.Server.csproj
```

### Running the Client

In a separate terminal, run the client project to open the Terminal User Interface (TUI).

```bash
dotnet run --project RosaDB.Client/RosaDB.Client.csproj
```

## Query Language at a Glance

RosaDB uses a custom SQL dialect designed around its context-based architecture.

**1. Define a Context Group (Schema for Contexts)**
```sql
CREATE CONTEXT sales (name TEXT PRIMARY KEY, region TEXT);
```

**2. Create a specific Context Instance**
```sql
INSERT CONTEXT sales (name, region) VALUES ('q4-2025', 'EMEA');
```

**3. Define a Table Schema for the Group**
```sql
CREATE TABLE sales.transactions (id INT PRIMARY KEY, product TEXT, amount INT);
```

**4. Insert Data into a specific Context Instance**
```sql
INSERT INTO sales.transactions USING name = 'q4-2025' (product, amount) VALUES ('Laptop', 1200);
```

**5. Query Data from a specific Context Instance**
```sql
SELECT * FROM sales.transactions USING name = 'q4-2025' WHERE amount > 1000;
```

**6. Query Data across ALL Context Instances in a Group**
```sql
SELECT AVG(amount) FROM sales.transactions WHERE product = 'Laptop';
```

**7. Subscribe to Real-time Changes**
(This would be sent over a WebSocket connection)
```sql
SUBSCRIBE TO sales USING name = 'q4-2025';
```

## Project Status

RosaDB is currently in development and is not yet production-ready. The concepts and APIs are subject to change.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.
