export type RowData = {
  [columnName: string]: any;
};

export type TableData = RowData[];

export interface ConnectionOptions {
  baseUrl: string;
  apiKey?: string;
  cacheTTL?: number; // optional cache duration in ms
}

export interface QueryResult {
  meta: {
    name: string;
    type: string;
  }[];
  data: any[][];
  rows: number;
  statistics: {
    elapsed: number;
    rows_read: number;
    bytes_read: number;
  };
}

function convertDuckDBValue(value: any, type: string) {
  if (value === null || value === undefined) return null;

  switch (type.toLowerCase()) {
    case 'boolean':
      return Boolean(value);

    case 'tinyint':
    case 'smallint':
    case 'integer':
    case 'int32':
    case 'float':
    case 'double':
    case 'decimal':
      return Number(value);

    case 'bigint':
    case 'int64':
    case 'hugeint':
      return BigInt(value);

    case 'varchar':
    case 'string':
    case 'text':
    case 'uuid':
      return String(value);

    case 'date':
      return new Date(value + 'T00:00:00Z');
    case 'time':
      return String(value);
    case 'timestamp':
    case 'datetime':
      return new Date(value.replace(' ', 'T') + 'Z');
    case 'timestamptz':
      return new Date(value);

    case 'blob':
      if (Buffer.isBuffer(value)) return value;
      if (typeof value === 'string') return Buffer.from(value, 'base64');
      return Buffer.from(value);

    case 'list':
    case 'array':
      return Array.isArray(value) ? value : JSON.parse(value);

    case 'struct':
    case 'json':
      return typeof value === 'object' ? value : JSON.parse(value);

    default:
      return value;
  }
}

class Connection {
  private baseUrl: string;
  private headers: HeadersInit;

  constructor(options: ConnectionOptions) {
    this.baseUrl = options.baseUrl.replace(/\/$/, '');
    this.headers = {};
    if (options.apiKey) {
      this.headers['X-API-Key'] = options.apiKey;
    }
  }

  async fetchNDJSON(sql: string): Promise<QueryResult> {
    const url = `${this.baseUrl}?query=${encodeURIComponent(
      sql
    )}&default_format=JSONCompact`;
    const res = await fetch(url, { headers: this.headers });

    if (!res.ok) {
      throw new Error(`DuckDB HTTP error ${res.status}: ${await res.text()}`);
    }
    return res.json();
  }
}

export class Database {
  private connection: Connection;
  private cacheTTL: number;
  private inFlight: Map<string, Promise<TableData>> = new Map();
  private resultCache: Map<string, { ts: number; data: TableData }> = new Map();

  private constructor(options: ConnectionOptions) {
    this.connection = new Connection(options);
    this.cacheTTL = options.cacheTTL ?? 0; // default: no caching
  }

  static async connect(options: ConnectionOptions): Promise<Database> {
    const db = new Database(options);
    await db.all('SELECT 1');
    return db;
  }

  private async _getQuery(sql: string): Promise<TableData> {
    const now = Date.now();

    // ✅ if cached result exists & still valid
    const cached = this.resultCache.get(sql);
    if (cached && this.cacheTTL > 0 && now - cached.ts < this.cacheTTL) {
      return cached.data;
    }

    // ✅ if promise in flight → return same promise
    if (this.inFlight.has(sql)) {
      return this.inFlight.get(sql)!;
    }

    const promise = (async () => {
      try {
        const { data: objects, meta: schema } =
          await this.connection.fetchNDJSON(sql);

        const rows: TableData = objects.map((obj) =>
          Object.fromEntries(
            schema.map((col, i) => [
              col.name,
              convertDuckDBValue(obj[i], col.type)
            ])
          )
        );

        // store cache if TTL enabled
        if (this.cacheTTL > 0) {
          this.resultCache.set(sql, { ts: now, data: rows });
        }

        return rows;
      } finally {
        // cleanup in-flight map
        this.inFlight.delete(sql);
      }
    })();

    this.inFlight.set(sql, promise);
    return promise;
  }

  async all(sql: string): Promise<TableData> {
    return this._getQuery(sql);
  }

  async each(sql: string, cb: (row: RowData) => void): Promise<void> {
    const rows = await this._getQuery(sql);
    for (const row of rows) cb(row);
  }

  async exec(sql: string): Promise<void> {
    await this._getQuery(sql);
  }

  async run(sql: string): Promise<void> {
    await this._getQuery(sql);
  }
}
