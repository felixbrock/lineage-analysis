import { Dependency, DependencyType } from '../entities/dependency';

export interface DependencyQueryDto {
  type?: DependencyType;
  headId?: string;
  tailId?: string;
  lineageId: string;
}

export interface IDependencyRepo {
  findOne(id: string): Promise<Dependency | null>;
  findBy(dependencyQueryDto: DependencyQueryDto): Promise<Dependency[]>;
  all(): Promise<Dependency[]>;
  insertOne(dependency: Dependency): Promise<string>;
  insertMany(dependencies: Dependency[]): Promise<string[]>;
  deleteOne(id: string): Promise<string>;
}
