import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';

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
    connPool: IConnectionPool
  ): Promise<Entity | undefined>;
  findBy(
    queryDto: QueryDto,
    auth: IAuth,
    connPool: IConnectionPool
  ): Promise<Entity[]>;
  findByCustom(
    query: { text: string; binds: IBinds },
    auth: IAuth,
    connPool: IConnectionPool
  ): Promise<Entity[]>;
  all(auth: IAuth, connPool: IConnectionPool): Promise<Entity[]>;
  insertOne(
    entity: Entity,
    auth: IAuth,
    connPool: IConnectionPool
  ): Promise<string>;
  insertMany(
    entities: Entity[],
    auth: IAuth,
    connPool: IConnectionPool
  ): Promise<string[]>;
  updateOne(
    id: string,
    updateDto: UpdateDto,
    auth: IAuth,
    connPool: IConnectionPool
  ): Promise<string>;
  replaceMany(
    entities: Entity[],
    auth: IAuth,
    connPool: IConnectionPool
  ): Promise<number>;
  deleteMany(
    ids: string[],
    auth: IAuth,
    connPool: IConnectionPool
  ): Promise<number>;
  deleteAll(auth: IAuth, connPool: IConnectionPool): Promise<void>;
}
