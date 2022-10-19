import { Lineage } from '../entities/lineage';
import { DbConnection } from '../services/i-db';

export interface LineageUpdateDto {
  completed?: boolean;
}

export interface ILineageRepo {
  findOne(dbConnection: DbConnection, id: string): Promise<Lineage | null>;
  findLatest(
    dbConnection: DbConnection,
    filter: {organizationId: string, completed?: boolean},
  ): Promise<Lineage | null>;
  all(dbConnection: DbConnection): Promise<Lineage[]>;
  insertOne(lineage: Lineage, dbConnection: DbConnection): Promise<string>;
  updateOne(
    id: string,
    updateDto: LineageUpdateDto,
    dbConnection: DbConnection
  ): Promise<string>;
  deleteOne(id: string, dbConnection: DbConnection): Promise<string>;
}
