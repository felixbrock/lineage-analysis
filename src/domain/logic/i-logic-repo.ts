import { Logic } from '../entities/logic';
import { IServiceRepo } from '../services/i-service-repo';

export type LogicUpdateDto = undefined;

export interface LogicQueryDto {
  relationNames?: string[];
}

export type ILogicRepo =  IServiceRepo<Logic, LogicQueryDto, LogicUpdateDto>;

