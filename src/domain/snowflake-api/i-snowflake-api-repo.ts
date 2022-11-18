import Result from '../value-types/transient-types/result';

export interface SnowflakeEntity {
  [key: string]: unknown;
}

export type SnowflakeQueryResult = SnowflakeEntity[];

export interface IConnectionPool {
  use<U>(cb: (resource: unknown) => U | Promise<U>): Promise<U>;
  drain(): Promise<void>;
}

export interface ISnowflakeApiRepo {
  runQuery(
    queryText: string,
    binds: (string | number)[] | (string | number)[][],
    connectionPool: IConnectionPool
  ): Promise<Result<SnowflakeQueryResult>>;
}
