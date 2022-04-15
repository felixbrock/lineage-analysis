import { Table } from '../entities/table';

export interface TableQueryDto {
  dbtModelId?: string;
  name?: string | string[];
  modelId?: string;
  lineageId: string;
}

export interface ITableRepo {
  findOne(id: string): Promise<Table | null>;
  findBy(tableQueryDto: TableQueryDto): Promise<Table[]>;
  all(): Promise<Table[]>;
  insertOne(table: Table): Promise<string>;
  deleteOne(id: string): Promise<string>;
}
