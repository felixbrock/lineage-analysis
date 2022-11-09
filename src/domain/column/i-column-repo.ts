import { Column } from '../entities/column';
import { DbConnection } from '../services/i-db';

export interface ColumnQueryDto {
  relationName?: string | string[];
  name?: string | string[];
  index?: string;
  type?: string;
  materializationId?: string | string[];
  lineageId: string;
  organizationId: string;
}

export interface ILegacyColumnRepo {
  findOne(id: string, dbConnection: DbConnection): Promise<Column | null>;
  findBy(columnQueryDto: ColumnQueryDto, dbConnection: DbConnection): Promise<Column[]>;
  all(dbConnection: DbConnection): Promise<Column[]>;
  insertOne(column: Column, dbConnection: DbConnection): Promise<string>;
  insertMany(columns: Column[], dbConnection: DbConnection): Promise<string[]>;
  replaceMany(columns: Column[], dbConnection: DbConnection): Promise<number>;
  deleteOne(id: string, dbConnection: DbConnection): Promise<string>;
}

export interface IColumnRepo {
  findOne(id: string, dbConnection: DbConnection): Promise<Column | null>;
  findBy(columnQueryDto: ColumnQueryDto, dbConnection: DbConnection): Promise<Column[]>;
  all(dbConnection: DbConnection): Promise<Column[]>;
  insertOne(column: Column, dbConnection: DbConnection): Promise<string>;
  insertMany(columns: Column[], dbConnection: DbConnection): Promise<string[]>;
  replaceMany(columns: Column[], dbConnection: DbConnection): Promise<number>;
  deleteOne(id: string, dbConnection: DbConnection): Promise<string>;
}
