import { Lineage } from '../entities/lineage';
import { DbConnection } from '../services/i-db';

export interface ILineageRepo {
  findOne(dbConnection: DbConnection, id: string): Promise<Lineage | null>;
  findCurrent(dbConnection: DbConnection, organizationId: string): Promise<Lineage | null>;
  all(dbConnection: DbConnection): Promise<Lineage[]>;
  insertOne(lineage: Lineage, dbConnection: DbConnection): Promise<string>;
  deleteOne(id: string, dbConnection: DbConnection): Promise<string>;
}
