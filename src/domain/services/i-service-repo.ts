import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import { IDbConnection } from './i-db';

export type IAuth = {
  jwt?: string;
  isSystemInternal?: boolean;
  callerOrgId?: string;
};

type IBinds = (string | number)[] | (string | number)[][];

export interface IServiceRepo<Entity, QueryDto, UpdateDto> {
  findOne(
    id: string,
    auth: IAuth,
    dbConnection: IDbConnection
  ): Promise<Entity | undefined>;
  findBy(
    queryDto: QueryDto,
    auth: IAuth,
    dbConnection: IDbConnection
  ): Promise<Entity[]>;
  findByCustom(
    query: { text: string; binds: IBinds },
    auth: IAuth,
    connPool: IConnectionPool
  ): Promise<Entity[]>;
  all(auth: IAuth, dbConnection: IDbConnection): Promise<Entity[]>;
  insertOne(
    entity: Entity,
    auth: IAuth,
    dbConnection: IDbConnection
  ): Promise<string>;
  insertMany(
    entities: Entity[],
    auth: IAuth,
    dbConnection: IDbConnection
  ): Promise<string[]>;
  updateOne(
    id: string,
    updateDto: UpdateDto,
    auth: IAuth,
    dbConnection: IDbConnection
  ): Promise<string>;
  replaceMany(
    entities: Entity[],
    auth: IAuth,
    dbConnection: IDbConnection
  ): Promise<number>;
  deleteMany(
    ids: string[],
    auth: IAuth,
    dbConnection: IDbConnection
  ): Promise<number>;
  deleteAll(auth: IAuth, dbConnection: IDbConnection): Promise<void>;
}
