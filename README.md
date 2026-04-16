# ChiralDB

**ChiralDB** is an autonomous, session-scoped database framework that transparently bridges the gap between Relational (SQL) and Document (JSONB) paradigms over a single PostgreSQL instance.

Instead of defining strict schemas or maintaining a separate MongoDB cluster for unstructured data, ChiralDB completely abstracts storage placement, schema evolution, and SQL JOINs away from the developer.

📚 **[Read the Full Documentation](https://devansh-lodha.github.io/chiral-db/)**

---

## ⚡ Key Features

* **Zero Schema Definition:** Ingest raw JSON. ChiralDB uses Shannon Entropy to autonomously infer data types and split repeating arrays into highly normalized SQL tables.
* **Hybrid Storage Engine:** Flat, stable scalars go to SQL columns. Drift-prone, heavily nested, or sparse data gracefully spills over into `JSONB` automatically.
* **Logical Session Isolation:** Data is physically stored in the same tables, but logically separated by `session_id`.
* **ACID Transactions:** No Two-Phase Commit (2PC) overhead. By utilizing PostgreSQL's JSONB alongside relational tables with `begin_nested()`, we achieve perfect Atomicity and Isolation across paradigms.
* **Built-in Dashboard:** Ships with a React SPA to visualize your logical schemas and execute CRUD operations.

---

## 🛠️ Quick Start

### Installation
```bash
pip install chiral-db
```

### Start the Server & Dashboard
ChiralDB ships with a built-in FastAPI server and React Dashboard. Just provide your PostgreSQL credentials in a `.env` file and run:
```bash
chiral serve --port 8000
```
Open `http://localhost:8000` in your browser to access the interactive Query Executor.

### Programmatic Usage
You can use ChiralDB natively in your Python `asyncio` applications. You don't write DDL. You don't write SQL. You just use data.

```python
import asyncio
from chiral.client import ChiralClient

async def main():
    async with ChiralClient("postgresql+asyncpg://user:pass@localhost:5432/db") as db:
        
        # 1. Ingest arbitrary, schema-less data
        await db.ingest(
            session_id="experiment_01", 
            data={
                "username": "devansh", 
                "sensors": [{"type": "temp", "val": 22}, {"type": "humid", "val": 40}]
            }
        )
        
        # 2. Query it logically. ChiralDB handles the SQL Joins and JSONB unpacking!
        result = await db.query({
            "operation": "read",
            "session_id": "experiment_01",
            "select": ["username", "sensors.val"],
            "filters": [{"field": "sensors.val", "op": "gt", "value": 20}]
        })
        
        print(result["rows"])

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 📄 License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
