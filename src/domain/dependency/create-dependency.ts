// todo clean architecture violation
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Dependency } from '../entities/dependency';
import { IDependencyRepo } from './i-dependency-repo';
import { ReadDependencies } from './read-dependencies';

export interface CreateDependencyRequestDto {
  type: string,
  headColumnId: string,
  tailColumnId: string,
  lineageId: string
}

export interface CreateDependencyAuthDto {
  organizationId: string;
}

export type CreateDependencyResponse = Result<Dependency>;

export class CreateDependency
  implements
    IUseCase<CreateDependencyRequestDto, CreateDependencyResponse, CreateDependencyAuthDto>
{
  readonly #readDependencies: ReadDependencies;

  readonly #dependencyRepo: IDependencyRepo;

  constructor(readDependencies: ReadDependencies, dependencyRepo: IDependencyRepo) {
    this.#readDependencies = readDependencies;
    this.#dependencyRepo = dependencyRepo;
  }

  async execute(
    request: CreateDependencyRequestDto,
    auth: CreateDependencyAuthDto
  ): Promise<CreateDependencyResponse> {
    console.log(auth);
    
    try {
      const dependency = Dependency.create({
        id: new ObjectId().toHexString(),
        type: request.type,
        headColumnId: request.headColumnId,
        tailColumnId: request.tailColumnId,
        lineageId: request.lineageId
      });


      const readColumnsResult = await this.#readDependencies.execute(
        {
          type: request.type,
          headColumnId: request.headColumnId,
          tailColumnId: request.tailColumnId,
          lineageId: request.lineageId,
        },
        { organizationId: auth.organizationId }
      );

      if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
      if (!readColumnsResult.value) throw new Error('Reading columns failed');
      if (readColumnsResult.value.length)
        throw new Error(`Column for table already exists`);

      await this.#dependencyRepo.insertOne(dependency);

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      return Result.ok(dependency);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
