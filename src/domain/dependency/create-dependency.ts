// todo clean architecture violation
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Dependency } from '../entities/dependency';
import { IDependencyRepo } from './i-dependency-repo';
import { ReadDependencies } from './read-dependencies';
import { ColumnRef } from '../entities/logic';
import { ReadColumns } from '../column/read-columns';
import { Column } from '../entities/column';
import { DbConnection } from '../services/i-db';

export interface CreateDependencyRequestDto {
  dependencyRef: ColumnRef;
  selfRelationName: string;
  parentRelationNames: string[];
  lineageId: string;
  writeToPersistence: boolean;
  targetOrganizationId?: string;
}

export interface CreateDependencyAuthDto {
  isSystemInternal: boolean;
  callerOrganizationId?: string;
}

export type CreateDependencyResponse = Result<Dependency>;

export class CreateDependency
  implements
    IUseCase<
      CreateDependencyRequestDto,
      CreateDependencyResponse,
      CreateDependencyAuthDto,
      DbConnection
    >
{
  readonly #readColumns: ReadColumns;

  readonly #readDependencies: ReadDependencies;

  readonly #dependencyRepo: IDependencyRepo;

  #dbConnection: DbConnection;

  /* Returns the object id of the parent column which self column depends upon */
  #getParentId = async (
    dependencyRef: ColumnRef,
    parentRelationNames: string[],
    lineageId: string,
    isSystemInternal: boolean,
    targetOrganizationId?: string,
    callerOrganizationId?: string
  ): Promise<string> => {
    const readColumnsResult = await this.#readColumns.execute(
      {
        relationName: parentRelationNames,
        name: dependencyRef.name,
        lineageId,
        targetOrganizationId,
      },
      { callerOrganizationId, isSystemInternal },
      this.#dbConnection
    );

    if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
    if (!readColumnsResult.value)
      throw new ReferenceError(`Reading of parent columns failed`);

    let potentialParents = readColumnsResult.value;

    if (!potentialParents.length)
      throw new ReferenceError('No parent found that matches reference');

    if (potentialParents.length > 1) {
      potentialParents = potentialParents.filter((parent) =>
        parent.relationName.includes(dependencyRef.materializationName)
      );
    }
    if (potentialParents.length !== 1)
      throw new ReferenceError('More than one matching parent');

    return potentialParents[0].id;
  };

  /* Returns column object that represents head of directed edge (the column that is referencing another column) */
  #getSelfColumn = async (
    selfRelationName: string,
    dependencyRef: ColumnRef,
    lineageId: string,
    isSystemInternal: boolean,
    targetOrganizationId?: string,
    callerOrganizationId?: string
  ): Promise<Column> => {
    const readSelfColumnResult = await this.#readColumns.execute(
      {
        relationName: selfRelationName,
        lineageId,
        name: dependencyRef.alias || dependencyRef.name,
        targetOrganizationId,
      },
      {
        callerOrganizationId,
        isSystemInternal,
      },
      this.#dbConnection
    );

    if (!readSelfColumnResult.success)
      throw new Error(readSelfColumnResult.error);
    if (!readSelfColumnResult.value)
      throw new ReferenceError(`Reading of dependency columns failed`);

    const selfColumnMatches = readSelfColumnResult.value;

    if (!selfColumnMatches.length) throw new RangeError('No self column found');

    if (selfColumnMatches.length === 1) return selfColumnMatches[0];

    throw new RangeError('0 or more than 1 selfColumns found');

    // const parentName: string = parentRef.name.includes('$')
    //   ? parentRef.name
    //   : parentRef.alias || parentRef.name;

    // const filterResult = readSelfColumnResult.value.filter(
    //   (column) => column.name === parentName
    // );

    // if (filterResult.length !== 1)
    //   throw new RangeError('0 or more than 1 selfColumns found');

    // return filterResult[0];
  };

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
    auth: CreateDependencyAuthDto,
    dbConnection: DbConnection
  ): Promise<CreateDependencyResponse> {
    try {
      if (auth.isSystemInternal && !request.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrganizationId && !auth.callerOrganizationId)
        throw new Error('No organization Id instance provided');
      if (request.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      this.#dbConnection = dbConnection;

      const headColumn = await this.#getSelfColumn(
        request.selfRelationName,
        request.dependencyRef,
        request.lineageId,
        auth.isSystemInternal,
        request.targetOrganizationId,
        auth.callerOrganizationId
      );

      // const parentName =
      //   request.parentRef.name.includes('$') && request.parentRef.alias
      //     ? request.parentRef.alias
      //     : request.parentRef.name;

      const parentId = await this.#getParentId(
        request.dependencyRef,
        request.parentRelationNames,
        request.lineageId,
        auth.isSystemInternal,
        request.targetOrganizationId,
        auth.callerOrganizationId
      );

      let organizationId: string;
      if (auth.isSystemInternal && request.targetOrganizationId)
        organizationId = request.targetOrganizationId;
      else if (!auth.isSystemInternal && auth.callerOrganizationId)
        organizationId = auth.callerOrganizationId;
      else throw new Error('Unhandled organization id declaration');
      
      const dependency = Dependency.create({
        id: new ObjectId().toHexString(),
        type: request.dependencyRef.dependencyType,
        headId: headColumn.id,
        tailId: parentId,
        lineageId: request.lineageId,
        organizationId,
      });

      const readDependencyResult = await this.#readDependencies.execute(
        {
          type: request.dependencyRef.dependencyType,
          headId: headColumn.id,
          tailId: parentId,
          lineageId: request.lineageId,
          targetOrganizationId: request.targetOrganizationId,
        },
        {
          isSystemInternal: auth.isSystemInternal,
          callerOrganizationId: auth.callerOrganizationId,
        },
        dbConnection
      );

      if (!readDependencyResult.success)
        throw new Error(readDependencyResult.error);
      if (!readDependencyResult.value)
        throw new Error('Creating dependency failed');
      if (readDependencyResult.value.length)
        throw new Error(
          `Attempting to create a dependency that already exists`
        );

      if (request.writeToPersistence)
        await this.#dependencyRepo.insertOne(dependency, this.#dbConnection);

      return Result.ok(dependency);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      console.warn('todo - fix. Creating dependency failed. Empty Result returned instead');
      // return Result.fail('');
      return Result.ok();
    }
  }
}
