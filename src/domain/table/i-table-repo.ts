import { Table } from "../entities/table";

export interface ITableRepo {
  findOne(id: string): Promise<Table | null>;
  all(): Promise<Table[]>;
  insertOne(table: Table): Promise<string>;
  deleteOne(id: string): Promise<string>;
}
