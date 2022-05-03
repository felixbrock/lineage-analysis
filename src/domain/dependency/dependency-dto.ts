import { Dependency, DependencyType } from '../entities/dependency';

export interface DependencyDto {
  id: string;
  type: DependencyType;
  headId: string;
  tailId: string;
  lineageId: string;
}

export const buildDependencyDto = (dependency: Dependency): DependencyDto => ({
  id: dependency.id,
  type: dependency.type,
  headId: dependency.headId,
  tailId: dependency.tailId,
  lineageId: dependency.lineageId,
});
