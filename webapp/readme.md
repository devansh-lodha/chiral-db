# ChiralDB React Flow Dashboard

This is the interactive visual frontend for the ChiralDB Autonomous Normalization Engine. It provides a real-time logical schema explorer and a fully-featured CRUD execution panel.

## Prerequisites
- [Node.js](https://nodejs.org/) (v18 or higher recommended)
- The ChiralDB backend API must be running locally on `http://127.0.0.1:8000` (e.g., via `just demo2`).

## Run with Docker (Recommended)

From the project root (`chiral-db`), run:

```bash
just webapp
```

Open your browser at [http://localhost:5173](http://localhost:5173).

To stop the dashboard container:

```bash
just webapp-stop
```

## Quick Start

If you want to run without Docker:

1. **Navigate to the dashboard directory**
   ```bash
   cd dashboard
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Start the development server**
   ```bash
   npm run dev
   ```

4. **Access the Dashboard**
   Open your browser to [http://localhost:5173](http://localhost:5173).
