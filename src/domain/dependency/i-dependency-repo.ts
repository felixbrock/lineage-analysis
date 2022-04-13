import { Dependency } from '../entities/dependency';

export interface DependencyQueryDto {
  type?: string;
  headColumnId?: string;
  tailColumnId?: string;
  lineageId: string;
}

export interface IDependencyRepo {
  findOne(id: string): Promise<Dependency | null>;
  findBy(dependencyQueryDto: DependencyQueryDto): Promise<Dependency[]>;
  all(): Promise<Dependency[]>;
  insertOne(dependency: Dependency): Promise<string>;
  deleteOne(id: string): Promise<string>;
}
