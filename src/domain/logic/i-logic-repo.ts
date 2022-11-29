import { Logic } from '../entities/logic';
import { IServiceRepo } from '../services/i-service-repo';

export type LogicUpdateDto = undefined;

export interface LogicQueryDto {
  relationName?: string;
  materializationIds?: string[];
}

export type ILogicRepo =  IServiceRepo<Logic, LogicQueryDto, LogicUpdateDto>;

