import { Materialization } from '../entities/materialization';

export interface MaterializationDto {
  id: string;
  relationName: string;
  materializationType: string;
  name: string;
  schemaName: string;
  databaseName: string;
  logicId?: string;
  lineageId: string;
  organizationId: string;
}

export const buildMaterializationDto = (
  materialization: Materialization
): MaterializationDto => ({
  id: materialization.id,
  relationName: materialization.relationName,
  materializationType: materialization.materializationType,
  name: materialization.name,
  schemaName: materialization.schemaName,
  databaseName: materialization.databaseName,
  logicId: materialization.logicId,
  lineageId: materialization.lineageId,
  organizationId: materialization.organizationId
});
