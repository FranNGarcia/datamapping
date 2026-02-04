interface ImportMeta {
  glob(pattern: string): Record<string, unknown>;
}

declare module "@aws-sdk/client-s3" {
  export class S3Client {
    constructor(config: any);
    send(command: any): Promise<any>;
  }

  export class ListObjectsV2Command {
    constructor(input: any);
  }

  export class GetObjectCommand {
    constructor(input: any);
  }
}

declare module "parquetjs-lite" {
  const parquet: any;
  export default parquet;
}

declare module "mssql" {
  const sql: any;
  export default sql;
}

declare module "mongodb" {
  export class MongoClient {
    constructor(uri: string, options?: any);
    connect(): Promise<MongoClient>;
    db(name?: string): any;
    close(): Promise<void>;
  }
}
