import { Logic } from '../entities/logic';
import { IBaseServiceRepo } from '../services/i-base-service-repo';

export type LogicUpdateDto = undefined;

export interface LogicQueryDto {
  relationName?: string;
  lineageId: string;
}

export type ILogicRepo =  IBaseServiceRepo<Logic, LogicQueryDto, LogicUpdateDto>;

