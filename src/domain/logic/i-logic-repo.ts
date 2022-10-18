import { Logic } from '../entities/logic';
import { DbConnection } from '../services/i-db';

export interface LogicQueryDto {
  relationName?: string;
  lineageIds: string[];
  organizationId: string;
}

export interface ILogicRepo {
  findOne(id: string, dbConnection: DbConnection): Promise<Logic | null>;
  findBy(materializationQueryDto: LogicQueryDto, dbConnection: DbConnection): Promise<Logic[]>;
  all(dbConnection: DbConnection): Promise<Logic[]>;
  insertOne(logic: Logic, dbConnection: DbConnection): Promise<string>;
  insertMany(logics: Logic[], dbConnection: DbConnection): Promise<string[]>;
  replaceMany(logics: Logic[], dbConnection: DbConnection): Promise<number>;
  deleteOne(id: string, dbConnection: DbConnection): Promise<string>;
}
