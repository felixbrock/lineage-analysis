import { Logic } from '../entities/logic';
import { DbConnection } from '../services/i-db';

export interface LogicQueryDto {
  dbtModelId?: string;
  lineageId: string;
}

export interface ILogicRepo {
  findOne(id: string, dbConnection: DbConnection): Promise<Logic | null>;
  findBy(materializationQueryDto: LogicQueryDto, dbConnection: DbConnection): Promise<Logic[]>;
  all(dbConnection: DbConnection): Promise<Logic[]>;
  insertOne(logic: Logic, dbConnection: DbConnection): Promise<string>;
  insertMany(logics: Logic[], dbConnection: DbConnection): Promise<string[]>;
  deleteOne(id: string, dbConnection: DbConnection): Promise<string>;
}
