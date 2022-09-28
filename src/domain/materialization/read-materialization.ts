import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { IMaterializationRepo } from './i-materialization-repo';
import {
  buildMaterializationDto,
  MaterializationDto,
} from './materialization-dto';
import { DbConnection } from '../services/i-db';

export interface ReadMaterializationRequestDto {
  id: string;
}

export interface ReadMaterializationAuthDto {
  callerOrganizationId: string;
}

export type ReadMaterializationResponseDto = Result<MaterializationDto>;

export class ReadMaterialization
  implements
    IUseCase<
      ReadMaterializationRequestDto,
      ReadMaterializationResponseDto,
      ReadMaterializationAuthDto,
      DbConnection
    >
{
  readonly #materializationRepo: IMaterializationRepo;

  #dbConnection: DbConnection;

  constructor(materializationRepo: IMaterializationRepo) {
    this.#materializationRepo = materializationRepo;
  }

  async execute(
    request: ReadMaterializationRequestDto,
    auth: ReadMaterializationAuthDto,
    dbConnection: DbConnection
  ): Promise<ReadMaterializationResponseDto> {
    ;

    try {
      this.#dbConnection = dbConnection;

      const materialization = await this.#materializationRepo.findOne(
        request.id,
        this.#dbConnection
      );
      if (!materialization)
        throw new Error(`Materialization with id ${request.id} does not exist`);

      if (materialization.organizationId !== auth.callerOrganizationId)
        throw new Error('Not authorized to perform action');

      return Result.ok(buildMaterializationDto(materialization));
    } catch (error: unknown) {
      if(error instanceof Error && error.message) console.trace(error.message); else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
