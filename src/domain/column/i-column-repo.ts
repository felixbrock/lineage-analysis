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
  callerOrgId: string;
  isSystemInternal: boolean;
}

export interface IColumnRepo {
  findOne(
    columnId: string,
    targetOrgId: string,
    auth: Auth
  ): Promise<Column | null>;
  findBy(
    columnQueryDto: ColumnQueryDto,
    targetOrgId: string,
    auth: Auth
  ): Promise<Column[]>;
  all(targetOrgId: string, auth: Auth): Promise<Column[]>;
  insertOne(column: Column, targetOrgId: string, auth: Auth): Promise<string>;
  insertMany(
    columns: Column[],
    targetOrgId: string,
    auth: Auth
  ): Promise<string[]>;
  replaceMany(
    columns: Column[],
    targetOrgId: string,
    auth: Auth
  ): Promise<number>;
}
