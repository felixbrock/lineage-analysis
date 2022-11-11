// todo - clean architecture violation

import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import {
  MaterializationType,
  Materialization,
} from '../entities/materialization';
import { ReadMaterializations } from './read-materializations';
import { IMaterializationRepo } from './i-materialization-repo';
import {  } from '../services/i-db';
import { v4 as uuidv4 } from 'uuid';

export interface CreateMaterializationRequestDto {
  id?: string;
  relationName: string;
  type: MaterializationType;
  name: string;
  schemaName: string;
  databaseName: string;
  lineageId: string;
  writeToPersistence: boolean;
  logicId?: string;
  ownerId?: string;
  isTransient?: boolean;
  comment?: string;
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
      
    >
{
  readonly #readMaterializations: ReadMaterializations;

  readonly #materializationRepo: IMaterializationRepo;

  #: ;

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
    : 
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

      this.# = ;

      const materialization = Materialization.create({
        id: request.id || uuidv4(),
        relationName: request.relationName,
        type: request.type,
        name: request.name,
        schemaName: request.schemaName,
        databaseName: request.databaseName,
        lineageId: request.lineageId,
        organizationId,
        logicId: request.logicId,
        ownerId: request.ownerId,
        isTransient: request.isTransient,
        comment: request.comment,
      });

      const readMaterializationsResult =
        await this.#readMaterializations.execute(
          {
            relationName: request.relationName,
            lineageId: request.lineageId,
            targetOrganizationId: request.targetOrganizationId,
          },
          {
            isSystemInternal: auth.isSystemInternal,
            callerOrganizationId: auth.callerOrganizationId,
          },
          this.#
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
          this.#
        );

      return Result.ok(materialization);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
