import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ILegacyMaterializationRepo } from './i-materialization-repo';
import { DbConnection } from '../services/i-db';
import { Materialization } from '../entities/materialization';

export interface ReadMaterializationRequestDto {
  id: string;
}

export interface ReadMaterializationAuthDto {
  callerOrganizationId: string;
}

export type ReadMaterializationResponseDto = Result<Materialization>;

export class ReadMaterialization
  implements
    IUseCase<
      ReadMaterializationRequestDto,
      ReadMaterializationResponseDto,
      ReadMaterializationAuthDto,
      DbConnection
    >
{
  readonly #materializationRepo: ILegacyMaterializationRepo;

  #dbConnection: DbConnection;

  constructor(materializationRepo: ILegacyMaterializationRepo) {
    this.#materializationRepo = materializationRepo;
  }

  async execute(
    request: ReadMaterializationRequestDto,
    auth: ReadMaterializationAuthDto,
    dbConnection: DbConnection
  ): Promise<ReadMaterializationResponseDto> {
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

      return Result.ok(materialization);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
