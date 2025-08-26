# duckdb-http

Connect to DuckDB over the HTTP protocol, allowing multiple processes to read and write concurrently.

## Installation

```bash
npm install duckdb-http
```

## Usage

```ts
import { Database } from 'duckdb-http';

async function main() {
  const db = await Database.connect({
    baseUrl: 'http://localhost:8888',
    apiKey: 'mysecret'
  });

  const rows = await db.all('SELECT * FROM transactions LIMIT 1');

  console.log('data:', rows);
}

main();
```

## License

MIT
