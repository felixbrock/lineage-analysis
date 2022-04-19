import { MaterializationType, Materialization } from '../entities/materialization';

export interface MaterializationQueryDto {
  dbtModelId?: string;
  materializationType?: MaterializationType;
  name?: string | string[];
  schemaName?: string;
  databaseName?: string;
  logicId?: string;
  lineageId: string;
}

export interface IMaterializationRepo {
  findOne(id: string): Promise<Materialization | null>;
  findBy(materializationQueryDto: MaterializationQueryDto): Promise<Materialization[]>;
  all(): Promise<Materialization[]>;
  insertOne(materialization: Materialization): Promise<string>;
  deleteOne(id: string): Promise<string>;
}
