import { Lineage } from '../entities/lineage';

export interface ILineageRepo {
  findOne(id: string): Promise<Lineage | null>;
  findCurrent(): Promise<Lineage | null>;
  all(): Promise<Lineage[]>;
  insertOne(lineage: Lineage): Promise<string>;
  deleteOne(id: string): Promise<string>;
}
