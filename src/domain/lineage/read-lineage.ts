import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ILineageRepo } from './i-lineage-repo';
import { Lineage } from '../entities/lineage';
import {  } from '../services/i-db';

export interface ReadLineageRequestDto {
  id?: string;
}

export interface ReadLineageAuthDto {
  callerOrganizationId: string;
}

export type ReadLineageResponseDto = Result<Lineage>;

export class ReadLineage
  implements
    IUseCase<
      ReadLineageRequestDto,
      ReadLineageResponseDto,
      ReadLineageAuthDto,
      
    >
{
  readonly #lineageRepo: ILineageRepo;

  #: ;

  constructor(lineageRepo: ILineageRepo) {
    this.#lineageRepo = lineageRepo;
  }

  async execute(
    request: ReadLineageRequestDto,
    auth: ReadLineageAuthDto,
    : 
  ): Promise<ReadLineageResponseDto> {
    try {
      this.# = ;

      const lineage = request.id
        ? await this.#lineageRepo.findOne(this.#, request.id)
        : await this.#lineageRepo.findLatest(this.#, {
            organizationId: auth.callerOrganizationId, completed: true
          });
      if (!lineage)
        throw new Error(
          `No lineage found for organization ${auth.callerOrganizationId}`
        );

      if (lineage.organizationId !== auth.callerOrganizationId)
        throw new Error('Not authorized to perform action');

      return Result.ok(lineage);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
