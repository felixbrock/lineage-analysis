import { Materialization } from '../entities/materialization';

export interface MaterializationDto {
  id: string;
  dbtModelId: string;
  materializationType: string;
  name: string;
  schemaName: string;
  databaseName: string;
  logicId: string;
}

export const buildMaterializationDto = (
  materialization: Materialization
): MaterializationDto => ({
  id: materialization.id,
  dbtModelId: materialization.dbtModelId,
  materializationType: materialization.materializationType,
  name: materialization.name,
  schemaName: materialization.schemaName,
  databaseName: materialization.databaseName,
  logicId: materialization.logicId,
});
