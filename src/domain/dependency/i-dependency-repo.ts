import { Dependency, DependencyType } from '../entities/dependency';
import { IServiceRepo } from '../services/i-base-service-repo';

export type DependencyUpdateDto = undefined;

export interface DependencyQueryDto {
  type?: DependencyType;
  headId?: string;
  tailId?: string;
  lineageId: string;
}

export type IDependencyRepo =  IServiceRepo<Dependency, DependencyQueryDto, DependencyUpdateDto>;