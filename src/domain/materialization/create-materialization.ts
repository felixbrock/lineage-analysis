// todo - clean architecture violation
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import {
  MaterializationType,
  Materialization,
} from '../entities/materialization';
import { ReadMaterializations } from './read-materializations';
import { IMaterializationRepo } from './i-materialization-repo';
import { DbConnection } from '../services/i-db';

export interface CreateMaterializationRequestDto {
  dbtModelId: string;
  materializationType: MaterializationType;
  name: string;
  schemaName: string;
  databaseName: string;
  logicId: string;
  lineageId: string;
  writeToPersistence: boolean;
  targetOrganizationId: string;
}

export interface CreateMaterializationAuthDto {
  isSystemInternal: boolean;
  callerOrganizationId: string;
}

export type CreateMaterializationResponseDto = Result<Materialization>;

export class CreateMaterialization
  implements
    IUseCase<
      CreateMaterializationRequestDto,
      CreateMaterializationResponseDto,
      CreateMaterializationAuthDto,
      DbConnection
    >
{
  readonly #readMaterializations: ReadMaterializations;

  readonly #materializationRepo: IMaterializationRepo;

  #dbConnection: DbConnection;

  constructor(
    readMaterializations: ReadMaterializations,
    materializationRepo: IMaterializationRepo
  ) {
    this.#readMaterializations = readMaterializations;
    this.#materializationRepo = materializationRepo;
  }

  async execute(
    request: CreateMaterializationRequestDto,
    auth: CreateMaterializationAuthDto,
    dbConnection: DbConnection
  ): Promise<CreateMaterializationResponseDto> {
    try {
      if (!auth.isSystemInternal) throw new Error('Unauthorized');

      this.#dbConnection = dbConnection;

      const materialization = Materialization.create({
        id: new ObjectId().toHexString(),
        dbtModelId: request.dbtModelId,
        materializationType: request.materializationType,
        name: request.name,
        schemaName: request.schemaName,
        databaseName: request.databaseName,
        logicId: request.logicId,
        lineageId: request.lineageId,
        organizationId: request.targetOrganizationId
      });

      const readMaterializationsResult =
        await this.#readMaterializations.execute(
          {
            dbtModelId: request.dbtModelId,
            lineageId: request.lineageId,
            targetOrganizationId: request.targetOrganizationId
          },
          { callerOrganizationId: auth.callerOrganizationId, isSystemInternal: auth.isSystemInternal }, this.#dbConnection
        );

      if (!readMaterializationsResult.success)
        throw new Error(readMaterializationsResult.error);
      if (!readMaterializationsResult.value)
        throw new Error('Reading materializations failed');
      if (readMaterializationsResult.value.length)
        throw new Error(`Materialization already exists`);

      if (request.writeToPersistence)
        await this.#materializationRepo.insertOne(
          materialization,
          this.#dbConnection
        );

      return Result.ok(materialization);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
