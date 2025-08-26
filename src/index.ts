export type RowData = {
  [columnName: string]: any;
};

export type TableData = RowData[];

export interface ConnectionOptions {
  baseUrl: string;
  apiKey?: string;
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

// Universal DuckDB -> JavaScript type mapper
function convertDuckDBValue(value: string, type: string) {
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
      // Use BigInt to preserve precision
      return BigInt(value);

    case 'varchar':
    case 'string':
    case 'text':
      return String(value);

    // ---- DATE / TIME / DATETIME ----
    case 'date': {
      // DuckDB DATE stored as YYYY-MM-DD
      return new Date(value + 'T00:00:00Z');
    }
    case 'time': {
      // DuckDB TIME stored as HH:MM:SS / HH:MM:SS.sss
      // Return a string to avoid losing precision
      return String(value);
    }
    case 'timestamp':
    case 'datetime': {
      // "YYYY-MM-DD HH:MM:SS" → valid ISO by replacing space with "T"
      return new Date(value.replace(' ', 'T') + 'Z');
    }
    case 'timestamptz': {
      // Already UTC-based in DuckDB
      return new Date(value);
    }

    case 'blob':
      if (Buffer.isBuffer(value)) return value; // already buffer
      if (typeof value === 'string') return Buffer.from(value, 'base64');
      return Buffer.from(value);

    case 'uuid':
      return String(value);

    case 'list':
    case 'array':
      return Array.isArray(value) ? value.map((v) => v) : JSON.parse(value);

    case 'struct':
      return typeof value === 'object' ? value : JSON.parse(value);

    case 'json':
      return JSON.parse(value);

    default:
      return value; // fallback: return as-is
  }
}

export class Connection {
  private baseUrl: string;
  private headers: HeadersInit;

  constructor(options: ConnectionOptions) {
    this.baseUrl = options.baseUrl.replace(/\/$/, ''); // strip trailing /
    this.headers = {};
    if (options.apiKey) {
      this.headers['X-API-Key'] = options.apiKey;
    }
  }

  private async _fetchNDJSON(sql: string): Promise<QueryResult> {
    const url = `${this.baseUrl}?query=${encodeURIComponent(
      sql
    )}&default_format=JSONCompact`;
    const res = await fetch(url, { headers: this.headers });

    if (!res.ok) {
      throw new Error(`DuckDB HTTP error ${res.status}: ${await res.text()}`);
    }

    return res.json();
  }

  private async _getQuery(sql: string): Promise<TableData> {
    const { data: objects, meta: schema } = await this._fetchNDJSON(sql);
    const rows = objects.map((obj) =>
      Object.fromEntries(
        schema.map((col, i) => [col.name, convertDuckDBValue(obj[i], col.type)])
      )
    );

    return rows;
  }

  async all(sql: string): Promise<TableData> {
    return this._getQuery(sql);
  }

  async each(sql: string, cb: (row: RowData) => void): Promise<void> {
    const data = await this._getQuery(sql);
    for (const row of data) cb(row);
  }

  async exec(sql: string): Promise<void> {
    await this._getQuery(sql);
  }

  async run(sql: string): Promise<void> {
    await this._getQuery(sql);
  }
}

export class Database {
  private options: ConnectionOptions;

  private constructor(options: ConnectionOptions) {
    this.options = options;
  }

  static async connect(options: ConnectionOptions): Promise<Database> {
    const db = new Database(options);
    // test connection
    await db.all('SELECT 1');
    return db;
  }

  connect(): Connection {
    return new Connection(this.options);
  }

  async all(sql: string): Promise<TableData> {
    return new Connection(this.options).all(sql);
  }

  async exec(sql: string): Promise<void> {
    return new Connection(this.options).exec(sql);
  }
}
