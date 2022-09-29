import {
  MaterializationType,
  Materialization,
} from '../entities/materialization';
import { DbConnection } from '../services/i-db';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import {
  IMaterializationRepo,
  MaterializationQueryDto,
} from './i-materialization-repo';

export interface ReadMaterializationsRequestDto {
  relationName?: string;
  materializationType?: MaterializationType;
  name?: string | string[];
  schemaName?: string;
  databaseName?: string;
  logicId?: string;
  lineageId: string;
  targetOrganizationId?: string;
}

export interface ReadMaterializationsAuthDto {
  callerOrganizationId?: string;
  isSystemInternal: boolean;
}

export type ReadMaterializationsResponseDto = Result<Materialization[]>;

export class ReadMaterializations
  implements
    IUseCase<
      ReadMaterializationsRequestDto,
      ReadMaterializationsResponseDto,
      ReadMaterializationsAuthDto,
      DbConnection
    >
{
  readonly #materializationRepo: IMaterializationRepo;

  #dbConnection: DbConnection;

  constructor(materializationRepo: IMaterializationRepo) {
    this.#materializationRepo = materializationRepo;
  }

  async execute(
    request: ReadMaterializationsRequestDto,
    auth: ReadMaterializationsAuthDto,
    dbConnection: DbConnection
  ): Promise<ReadMaterializationsResponseDto> {
    try {
      if (auth.isSystemInternal && !request.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrganizationId && !auth.callerOrganizationId)
        throw new Error('No organization Id instance provided');
      if (request.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      let organizationId;
      if (auth.isSystemInternal && request.targetOrganizationId)
        organizationId = request.targetOrganizationId;
      else if (auth.callerOrganizationId)
        organizationId = auth.callerOrganizationId;
      else throw new Error('Unhandled organizationId allocation');

      this.#dbConnection = dbConnection;

      const materializations: Materialization[] =
        await this.#materializationRepo.findBy(
          this.#buildMaterializationQueryDto(request, organizationId),
          this.#dbConnection
        );
      if (!materializations)
        throw new Error(`Queried materializations do not exist`);

      return Result.ok(materializations);
    } catch (error: unknown) {
      if(error instanceof Error && error.message) console.trace(error.message); else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }

  #buildMaterializationQueryDto = (
    request: ReadMaterializationsRequestDto,
    organizationId: string
  ): MaterializationQueryDto => {
    const queryDto: MaterializationQueryDto = {
      lineageId: request.lineageId,
      organizationId,
    };

    if (request.relationName) queryDto.relationName = request.relationName;
    if (request.name) queryDto.name = request.name;
    if (request.logicId) queryDto.logicId = request.logicId;

    return queryDto;
  };
}
