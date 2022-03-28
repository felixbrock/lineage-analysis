import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { IModelRepo } from './i-model-repo';
import { Model } from '../entities/model';

export interface ReadModelRequestDto {
  id: string;
}

export interface ReadModelAuthDto {
  organizationId: string;
}

export type ReadModelResponseDto = Result<Model>;

export class ReadModel
  implements
    IUseCase<ReadModelRequestDto, ReadModelResponseDto, ReadModelAuthDto>
{
  #modelRepo: IModelRepo;

  constructor(modelRepo: IModelRepo) {
    this.#modelRepo = modelRepo;
  }

  async execute(
    request: ReadModelRequestDto,
    auth: ReadModelAuthDto
  ): Promise<ReadModelResponseDto> {
    try {
      // todo -replace
      console.log(auth);

      const model = await this.#modelRepo.findOne(request.id);
      if (!model) throw new Error(`Model with id ${request.id} does not exist`);

      // if (model.organizationId !== auth.organizationId)
      //   throw new Error('Not authorized to perform action');

      return Result.ok(model);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
