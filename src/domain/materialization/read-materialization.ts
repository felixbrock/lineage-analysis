import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { IMaterializationRepo } from './i-materialization-repo';

import { Materialization } from '../entities/materialization';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';

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
      ReadMaterializationAuthDto,IConnectionPool
    >
{
  readonly #materializationRepo: IMaterializationRepo;

  constructor(materializationRepo: IMaterializationRepo) {
    this.#materializationRepo = materializationRepo;
  }

  async execute(
    request: ReadMaterializationRequestDto,
    auth: ReadMaterializationAuthDto,
    connPool: IConnectionPool
  ): Promise<ReadMaterializationResponseDto> {
    try {
      const materialization = await this.#materializationRepo.findOne(
        request.id,
        auth,
        connPool,
      );
      if (!materialization)
        throw new Error(`Materialization with id ${request.id} does not exist`);

      return Result.ok(materialization);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.error(error.stack);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
