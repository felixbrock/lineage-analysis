import { Dependency } from '../value-types/dependency';

export interface DependencyDto {
  type: string;
  columnId: string;
  direction: string;
}

export const buildDependencyDto = (dependency: Dependency): DependencyDto => ({
  type: dependency.type,
  columnId: dependency.columnId,
  direction: dependency.direction
});