import { Model } from '../entities/model';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { IModelRepo, ModelQueryDto } from './i-model-repo';

export interface ReadModelsRequestDto {
  location?: string;
}

export interface ReadModelsAuthDto {
  organizationId: string;
}

export type ReadModelsResponseDto = Result<Model[]>;

export class ReadModels
  implements
    IUseCase<ReadModelsRequestDto, ReadModelsResponseDto, ReadModelsAuthDto>
{
  #modelRepo: IModelRepo;

  public constructor(modelRepo: IModelRepo) {
    this.#modelRepo = modelRepo;
  }

  public async execute(
    request: ReadModelsRequestDto,
    auth: ReadModelsAuthDto
  ): Promise<ReadModelsResponseDto> {
    try {
      const models: Model[] = await this.#modelRepo.findBy(
        this.#buildModelQueryDto(request, auth.organizationId)
      );
      if (!models) throw new Error(`Queried models do not exist`);

      return Result.ok(models);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }

  #buildModelQueryDto = (
    request: ReadModelsRequestDto,
    organizationId: string
  ): ModelQueryDto => {
    const queryDto: ModelQueryDto = {};

    // todo - add organizationId
    // queryDto.organizationId = organizationId;
    if (request.location) queryDto.location = request.location;

    return queryDto;
  };
}
