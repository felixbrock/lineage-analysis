import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import {
  DeletionMode,
  IObservabilityApiRepo,
} from './i-observability-api-repo';

export type DeleteTestSuitesRequestDto = {
  targetResourceIds: string[];
  mode: DeletionMode;
};

export interface DeleteTestSuitesAuthDto {
  jwt: string;
}

export type DeleteTestSuitesResponseDto = Result<undefined>;

export class DeleteTestSuites
  implements
    IUseCase<
      DeleteTestSuitesRequestDto,
      DeleteTestSuitesResponseDto,
      DeleteTestSuitesAuthDto
    >
{
  readonly #observabilityApiRepo: IObservabilityApiRepo;

  constructor(observabilityApiRepo: IObservabilityApiRepo) {
    this.#observabilityApiRepo = observabilityApiRepo;
  }

  async execute(
    request: DeleteTestSuitesRequestDto,
    auth: DeleteTestSuitesAuthDto
  ): Promise<DeleteTestSuitesResponseDto> {
    try {
      await Promise.all([
        this.#observabilityApiRepo.deleteQuantTestSuites(
          auth.jwt,
          request.targetResourceIds,
          request.mode
        ),
        this.#observabilityApiRepo.deleteQualTestSuites(
          auth.jwt,
          request.targetResourceIds,
          request.mode
        ),
      ]);

      return Result.ok();
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
