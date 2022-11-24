import {
  MaterializationType,
  Materialization,
} from '../entities/materialization';
import BaseAuth from '../services/base-auth';
import IUseCase from '../services/use-case';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import Result from '../value-types/transient-types/result';
import {
  IMaterializationRepo,
  MaterializationQueryDto,
} from './i-materialization-repo';

export interface ReadMaterializationsRequestDto {
  relationName?: string;
  materializationType?: MaterializationType;
  names?: string[];
  schemaName?: string;
  databaseName?: string;
  logicId?: string;
  lineageId: string;
  targetOrgId?: string;
  
}

export type ReadMaterializationsAuthDto = BaseAuth


export type ReadMaterializationsResponseDto = Result<Materialization[]>;

export class ReadMaterializations
  implements
    IUseCase<
      ReadMaterializationsRequestDto,
      ReadMaterializationsResponseDto,
      ReadMaterializationsAuthDto,IConnectionPool
    >
{
  readonly #materializationRepo: IMaterializationRepo;

  constructor(
    materializationRepo: IMaterializationRepo,
  ) {
    this.#materializationRepo = materializationRepo;
  }

  async execute(
    req: ReadMaterializationsRequestDto,
    auth: ReadMaterializationsAuthDto,
    connPool: IConnectionPool
  ): Promise<ReadMaterializationsResponseDto> {
    try {
      if (auth.isSystemInternal && !req.targetOrgId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrgId)
        throw new Error('Caller organization id missing');
      if (!req.targetOrgId && !auth.callerOrgId)
        throw new Error('No organization Id instance provided');
      if (req.targetOrgId && auth.callerOrgId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const materializations: Materialization[] =
        await this.#materializationRepo.findBy(
          this.#buildMaterializationQueryDto(req),
          auth,
          connPool,
        );
      if (!materializations)
        throw new Error(`Queried materializations do not exist`);

      return Result.ok(materializations);
    } catch (error: unknown) {
      if (error instanceof Error ) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }

  #buildMaterializationQueryDto = (
    request: ReadMaterializationsRequestDto
  ): MaterializationQueryDto => {
    const queryDto: MaterializationQueryDto = {
      lineageId: request.lineageId,
    };

    if (request.relationName) queryDto.relationName = request.relationName;
    if (request.names) queryDto.names = request.names;
    if (request.logicId) queryDto.logicId = request.logicId;

    return queryDto;
  };
}
