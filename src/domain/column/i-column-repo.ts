import { Column } from '../entities/column';

export interface ColumnQueryDto {
  relationName?: string | string[];
  name?: string | string[];
  index?: string;
  type?: string;
  materializationId?: string | string[];
  lineageId: string;
}
export interface Auth {
  jwt: string;
  callerOrgId?: string;
  isSystemInternal: boolean;
}

export interface IColumnRepo {
  findOne(
    columnId: string,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Column | null>;
  findBy(
    columnQueryDto: ColumnQueryDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Column[]>;
  all(auth: Auth, targetOrgId?: string): Promise<Column[]>;
  insertOne(column: Column, auth: Auth, targetOrgId?: string): Promise<string>;
  insertMany(
    columns: Column[],
    auth: Auth,
    targetOrgId?: string
  ): Promise<string[]>;
  replaceMany(
    columns: Column[],
    auth: Auth,
    targetOrgId?: string
  ): Promise<number>;
}
