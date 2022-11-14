import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { IMaterializationRepo } from './i-materialization-repo';
import {} from '../services/i-db';
import { Materialization } from '../entities/materialization';

export interface ReadMaterializationRequestDto {
  id: string;
  targetOrgId?: string;
}

export interface ReadMaterializationAuthDto {
  callerOrgId: string;
  isSystemInternal: boolean;
  jwt: string;
}

export type ReadMaterializationResponseDto = Result<Materialization>;

export class ReadMaterialization
  implements
    IUseCase<
      ReadMaterializationRequestDto,
      ReadMaterializationResponseDto,
      ReadMaterializationAuthDto
    >
{
  readonly #materializationRepo: IMaterializationRepo;

  constructor(materializationRepo: IMaterializationRepo) {
    this.#materializationRepo = materializationRepo;
  }

  async execute(
    request: ReadMaterializationRequestDto,
    auth: ReadMaterializationAuthDto
  ): Promise<ReadMaterializationResponseDto> {
    try {
      const materialization = await this.#materializationRepo.findOne(
        request.id,
        auth,
        request.targetOrgId
      );
      if (!materialization)
        throw new Error(`Materialization with id ${request.id} does not exist`);

      return Result.ok(materialization);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
