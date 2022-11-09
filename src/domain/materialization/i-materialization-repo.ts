import { MaterializationType, Materialization } from '../entities/materialization';
import { DbConnection } from '../services/i-db';

export interface MaterializationQueryDto {
  relationName?: string;
  materializationType?: MaterializationType;
  name?: string | string[];
  schemaName?: string;
  databaseName?: string;
  logicId?: string;
  lineageId: string;
  organizationId: string;
}

export interface ILegacyMaterializationRepo {
  findOne(id: string, dbConnection: DbConnection): Promise<Materialization | null>;
  findBy(materializationQueryDto: MaterializationQueryDto, dbConnection: DbConnection): Promise<Materialization[]>;
  all(dbConnection: DbConnection): Promise<Materialization[]>;
  insertOne(materialization: Materialization, dbConnection: DbConnection): Promise<string>;
  insertMany(materializations: Materialization[], dbConnection: DbConnection): Promise<string[]>;
  replaceMany(materializations: Materialization[], dbConnection: DbConnection): Promise<number>;
  deleteOne(id: string, dbConnection: DbConnection): Promise<string>;
}

export interface IMaterializationRepo {
  findOne(id: string, dbConnection: DbConnection): Promise<Materialization | null>;
  findBy(materializationQueryDto: MaterializationQueryDto, dbConnection: DbConnection): Promise<Materialization[]>;
  all(dbConnection: DbConnection): Promise<Materialization[]>;
  insertOne(materialization: Materialization, dbConnection: DbConnection): Promise<string>;
  insertMany(materializations: Materialization[], dbConnection: DbConnection): Promise<string[]>;
  replaceMany(materializations: Materialization[], dbConnection: DbConnection): Promise<number>;
  deleteOne(id: string, dbConnection: DbConnection): Promise<string>;
}


