import { Logic } from '../entities/logic';
import { IServiceRepo } from '../services/i-base-service-repo';

export type LogicUpdateDto = undefined;

export interface LogicQueryDto {
  relationName?: string;
  lineageId: string;
}

export type ILogicRepo =  IServiceRepo<Logic, LogicQueryDto, LogicUpdateDto>;

