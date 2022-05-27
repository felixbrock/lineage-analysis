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
  dbtModelId?: string;
  materializationType?: MaterializationType;
  name?: string | string[];
  schemaName?: string;
  databaseName?: string;
  logicId?: string;
  lineageId: string;
}

export interface ReadMaterializationsAuthDto {
  organizationId: string;
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
      this.#dbConnection = dbConnection;

      const materializations: Materialization[] =
        await this.#materializationRepo.findBy(
          this.#buildMaterializationQueryDto(request, auth.organizationId),
          this.#dbConnection
        );
      if (!materializations)
        throw new Error(`Queried materializations do not exist`);

      return Result.ok(materializations);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }

  #buildMaterializationQueryDto = (
    request: ReadMaterializationsRequestDto,
    organizationId: string
  ): MaterializationQueryDto => {
    console.log(organizationId);

    const queryDto: MaterializationQueryDto = { lineageId: request.lineageId };

    // todo - add organizationId
    // queryDto.organizationId = organizationId;
    if (request.dbtModelId) queryDto.dbtModelId = request.dbtModelId;
    if (request.name) queryDto.name = request.name;
    if (request.logicId) queryDto.logicId = request.logicId;

    return queryDto;
  };
}
