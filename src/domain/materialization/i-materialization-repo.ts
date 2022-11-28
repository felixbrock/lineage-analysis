import { Materialization, MaterializationType } from '../entities/materialization';
import { IServiceRepo } from '../services/i-service-repo';

export type MaterializationUpdateDto = undefined;

export interface MaterializationQueryDto {
  ids?: string[];
  relationName?: string;
  type?: MaterializationType;
  names?: string[];
  schemaName?: string;
  databaseName?: string;
  logicId?: string;
  lineageId: string;
}

export type IMaterializationRepo =  IServiceRepo<Materialization, MaterializationQueryDto, MaterializationUpdateDto>;

