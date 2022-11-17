import Result from '../value-types/transient-types/result';

export interface SnowflakeEntity {
  [key: string]: unknown;
}

export type SnowflakeQueryResult = SnowflakeEntity[];

export interface ConnectionPool {
  use: unknown;
  destroy: unknown;
}

export interface ISnowflakeApiRepo {
  runQuery(
    queryText: string,
    binds: (string | number)[] | (string | number)[][],
    connectionPool: ConnectionPool
  ): Promise<Result<SnowflakeQueryResult>>;
}
