// todo - clean architecture violation

import { v4 as uuidv4 } from 'uuid';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import {
  MaterializationType,
  Materialization,
} from '../entities/materialization';
import { ReadMaterializations } from './read-materializations';
import { IMaterializationRepo } from './i-materialization-repo';
import BaseAuth from '../services/base-auth';
import { IDbConnection } from '../services/i-db';

export interface CreateMaterializationRequestDto {
  id?: string;
  relationName: string;
  type: MaterializationType;
  name: string;
  schemaName: string;
  databaseName: string;
  writeToPersistence: boolean;
  logicId?: string;
  ownerId?: string;
  isTransient?: boolean;
  comment?: string;
  targetOrgId?: string;
}

export type CreateMaterializationAuthDto = BaseAuth;

export type CreateMaterializationResponseDto = Result<Materialization>;

export class CreateMaterialization
  implements
    IUseCase<
      CreateMaterializationRequestDto,
      CreateMaterializationResponseDto,
      CreateMaterializationAuthDto, IDbConnection
    >
{
  readonly #readMaterializations: ReadMaterializations;

  readonly #materializationRepo: IMaterializationRepo;

  constructor(
    readMaterializations: ReadMaterializations,
    materializationRepo: IMaterializationRepo
  ) {
    this.#readMaterializations = readMaterializations;
    this.#materializationRepo = materializationRepo;
  }

  async execute(
    req: CreateMaterializationRequestDto,
    auth: CreateMaterializationAuthDto,
    dbConnection: IDbConnection
  ): Promise<CreateMaterializationResponseDto> {
    try {
      if (auth.isSystemInternal && !req.targetOrgId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrgId)
        throw new Error('Caller organization id missing');
      if (!req.targetOrgId && !auth.callerOrgId)
        throw new Error('No organization Id instance provided');
      if (req.targetOrgId && auth.callerOrgId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const materialization = Materialization.create({
        id: req.id || uuidv4(),
        relationName: req.relationName,
        type: req.type,
        name: req.name,
        schemaName: req.schemaName,
        databaseName: req.databaseName,
        logicId: req.logicId,
        ownerId: req.ownerId,
        isTransient: req.isTransient,
        comment: req.comment,
      });

      if (req.writeToPersistence)
        await this.#materializationRepo.insertOne(
          materialization,
          auth,
          dbConnection,
        );

      return Result.ok(materialization);
    } catch (error: unknown) {
      if (error instanceof Error ) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
