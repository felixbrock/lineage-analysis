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
  modelId: string;
  materializationType: MaterializationType;
  name: string;
  schemaName: string;
  databaseName: string;
  logicId: string;
  lineageId: string;
  writeToPersistence: boolean;
  targetOrganizationId?: string;
}

export interface CreateMaterializationAuthDto {
  isSystemInternal: boolean;
  callerOrganizationId?: string;
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
      if (auth.isSystemInternal && !request.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrganizationId && !auth.callerOrganizationId)
        throw new Error('No organization Id instance provided');
      if (request.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      let organizationId: string;
      if (auth.isSystemInternal && request.targetOrganizationId)
        organizationId = request.targetOrganizationId;
      else if (!auth.isSystemInternal && auth.callerOrganizationId)
        organizationId = auth.callerOrganizationId;
      else throw new Error('Unhandled organization id declaration');

      this.#dbConnection = dbConnection;

      const materialization = Materialization.create({
        id: new ObjectId().toHexString(),
        modelId: request.modelId,
        materializationType: request.materializationType,
        name: request.name,
        schemaName: request.schemaName,
        databaseName: request.databaseName,
        logicId: request.logicId,
        lineageId: request.lineageId,
        organizationId,
      });

      const readMaterializationsResult =
        await this.#readMaterializations.execute(
          {
            modelId: request.modelId,
            lineageId: request.lineageId,
            targetOrganizationId: request.targetOrganizationId,
          },
          { isSystemInternal: auth.isSystemInternal, callerOrganizationId: auth.callerOrganizationId },
          this.#dbConnection
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
      if(error instanceof Error && error.message) console.trace(error.message); else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
