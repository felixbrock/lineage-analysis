import { Dependency, DependencyType } from '../entities/dependency';
import { IServiceRepo } from '../services/i-service-repo';

export type DependencyUpdateDto = undefined;

export interface DependencyQueryDto {
  type?: DependencyType;
  headId?: string;
  tailId?: string;
}

export type IDependencyRepo =  IServiceRepo<Dependency, DependencyQueryDto, DependencyUpdateDto>;