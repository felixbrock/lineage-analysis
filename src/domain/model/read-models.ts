import { Model } from '../entities/model';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { IModelRepo, ModelQueryDto } from './i-model-repo';

export interface ReadModelsRequestDto {
  dbtModelId?: string;
  lineageId: string;
}

export interface ReadModelsAuthDto {
  organizationId: string;
}

export type ReadModelsResponseDto = Result<Model[]>;

export class ReadModels
  implements
    IUseCase<ReadModelsRequestDto, ReadModelsResponseDto, ReadModelsAuthDto>
{
  readonly #modelRepo: IModelRepo;

  constructor(modelRepo: IModelRepo) {
    this.#modelRepo = modelRepo;
  }

  async execute(
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
    console.log(organizationId);

    const queryDto: ModelQueryDto = { lineageId: request.lineageId };

    // todo - add organizationId
    // queryDto.organizationId = organizationId;
    if (request.dbtModelId) queryDto.dbtModelId = request.dbtModelId;

    return queryDto;
  };
}
