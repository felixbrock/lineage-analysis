// todo clean architecture violation
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Dependency } from '../entities/dependency';
import { IDependencyRepo } from './i-dependency-repo';
import { ReadDependencies } from './read-dependencies';
import { ColumnRef } from '../entities/logic';
import { ReadColumns } from '../column/read-columns';

export interface CreateDependencyRequestDto {
  selfRef: ColumnRef;
  selfDbtModelId: string;
  parentRef: ColumnRef;
  parentDbtModelIds: string[];
  lineageId: string;
}

export interface CreateDependencyAuthDto {
  organizationId: string;
}

export type CreateDependencyResponse = Result<Dependency>;

export class CreateDependency
  implements
    IUseCase<
      CreateDependencyRequestDto,
      CreateDependencyResponse,
      CreateDependencyAuthDto
    >
{
  readonly #readColumns: ReadColumns;

  readonly #readDependencies: ReadDependencies;

  readonly #dependencyRepo: IDependencyRepo;

  #getParentId = async (
    parentDbtModelIds: string[],
    parentName: string,
    lineageId: string
  ): Promise<string> => {
    const readColumnsResult = await this.#readColumns.execute(
      { dbtModelId: parentDbtModelIds, name: parentName, lineageId },
      { organizationId: 'todo' }
    );

    if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
    if (!readColumnsResult.value)
      throw new ReferenceError(`Reading of dependency columns failed`);

    const potentialParents = readColumnsResult.value;

    if (!potentialParents.length)
      throw new ReferenceError('No parent found that matches reference');

    if (potentialParents.length > 1)
      throw new ReferenceError('More than one matching parent');

    return potentialParents[0].id;
  };

  #getMatchingDbtModelIds = (
    parentDbtModelIds: string[],
    parentMaterializationName: string
  ): string[] =>
    parentDbtModelIds.filter((id) =>
      id.toLowerCase().includes(parentMaterializationName.toLowerCase())
    );

  constructor(
    readColumns: ReadColumns,
    readDependencies: ReadDependencies,
    dependencyRepo: IDependencyRepo
  ) {
    this.#readColumns = readColumns;
    this.#readDependencies = readDependencies;
    this.#dependencyRepo = dependencyRepo;
  }

  async execute(
    request: CreateDependencyRequestDto,
    auth: CreateDependencyAuthDto
  ): Promise<CreateDependencyResponse> {
    console.log(auth);

    try {
      const readSelfColumnResult = await this.#readColumns.execute(
        { dbtModelId: request.selfDbtModelId, lineageId: request.lineageId },
        { organizationId: 'todo' }
      );

      if (!readSelfColumnResult.success)
        throw new Error(readSelfColumnResult.error);
      if (!readSelfColumnResult.value)
        throw new ReferenceError(`Reading of dependency columns failed`);

      const [selfColumn] = readSelfColumnResult.value;

      const matchingDbtModelIds = this.#getMatchingDbtModelIds(request.parentDbtModelIds, request.parentRef.materializationName);

      if(!matchingDbtModelIds.length) throw new ReferenceError('No matching dbt model id found for dependency to create');

      const parentId = await this.#getParentId(
        matchingDbtModelIds,
        request.parentRef.name,
        request.lineageId
      );

      const dependency = Dependency.create({
        id: new ObjectId().toHexString(),
        type: request.selfRef.dependencyType,
        headColumnId: selfColumn.id,
        tailColumnId: parentId,
        lineageId: request.lineageId,
      });

      const readColumnsResult = await this.#readDependencies.execute(
        {
          type: request.selfRef.dependencyType,
          headColumnId: selfColumn.id,
          tailColumnId: parentId,
          lineageId: request.lineageId,
        },
        { organizationId: auth.organizationId }
      );

      if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
      if (!readColumnsResult.value) throw new Error('Reading columns failed');
      if (readColumnsResult.value.length)
        throw new Error(`Column for materialization already exists`);

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
