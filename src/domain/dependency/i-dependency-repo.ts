import { Dependency, DependencyType } from '../entities/dependency';
import { DbConnection } from '../services/i-db';

export interface DependencyQueryDto {
  type?: DependencyType;
  headId?: string;
  tailId?: string;
  lineageId: string;
}

export interface IDependencyRepo {
  findOne(id: string, dbConnection: DbConnection): Promise<Dependency | null>;
  findBy(dependencyQueryDto: DependencyQueryDto, dbConnection: DbConnection): Promise<Dependency[]>;
  all(dbConnection: DbConnection): Promise<Dependency[]>;
  insertOne(dependency: Dependency, dbConnection: DbConnection): Promise<string>;
  insertMany(dependencies: Dependency[], dbConnection: DbConnection): Promise<string[]>;
  deleteOne(id: string, dbConnection: DbConnection): Promise<string>;
}
