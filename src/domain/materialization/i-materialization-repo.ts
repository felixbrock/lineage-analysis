import { Materialization, MaterializationType } from '../entities/materialization';
import { IBaseServiceRepo } from '../services/i-base-service-repo';

export type MaterializationUpdateDto = undefined;

export interface MaterializationQueryDto {
  relationName?: string;
  type?: MaterializationType;
  name?: string | string[];
  schemaName?: string;
  databaseName?: string;
  logicId?: string;
  lineageId: string;
}

export type IMaterializationRepo =  IBaseServiceRepo<Materialization, MaterializationQueryDto, MaterializationUpdateDto>;

