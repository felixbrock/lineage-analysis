import { Column } from '../entities/column';

export interface ColumnQueryDto {
  name?: string | string[];
  tableId?: string | string[];
  lineageId: string;
}

export interface IColumnRepo {
  findOne(id: string): Promise<Column | null>;
  findBy(columnQueryDto: ColumnQueryDto): Promise<Column[]>;
  all(): Promise<Column[]>;
  insertOne(column: Column): Promise<string>;
  deleteOne(id: string): Promise<string>;
}
