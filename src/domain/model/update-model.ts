import IUseCase from '../services/use-case';
import { IModelRepo, ModelUpdateDto } from './i-model-repo';
import Result from '../value-types/transient-types/result';
import { ReadModels, ReadModelsResponseDto } from './read-models';
import { Model } from '../entities/model';
import { ReadModel } from './read-model';

export interface UpdateModelRequestDto {
  id: string;
  location?: string;
  parsedLogic?: string;
}

export interface UpdateModelAuthDto {
  organizationId: string;
}

export type UpdateModelResponseDto = Result<Model>;

export class UpdateModel
  implements
    IUseCase<UpdateModelRequestDto, UpdateModelResponseDto, UpdateModelAuthDto>
{
  #modelRepo: IModelRepo;

  #readModel: ReadModel;

  #readModels: ReadModels;

  constructor(
    modelRepo: IModelRepo,
    readModel: ReadModel,
    readModels: ReadModels
  ) {
    this.#modelRepo = modelRepo;
    this.#readModel = readModel;
    this.#readModels = readModels;
  }

  async execute(
    request: UpdateModelRequestDto,
    auth: UpdateModelAuthDto
  ): Promise<UpdateModelResponseDto> {
    try {
      const readModelResult = await this.#readModel.execute(
        { id: request.id },
        { organizationId: auth.organizationId }
      );

      if (!readModelResult.success) throw new Error(readModelResult.error);

      if (!readModelResult.value)
        throw new Error(`Model with id ${request.id} does not exist`);

      // if (readModelResult.value.organizationId !== auth.organizationId)
      //   throw new Error('Not authorized to perform action');

      const updateDto = await this.#buildUpdateDto(
        request,
        readModelResult.value,
        auth.organizationId
      );

      await this.#modelRepo.updateOne(request.id, updateDto);

      return Result.ok(readModelResult.value);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }

  #buildUpdateDto = async (
    request: UpdateModelRequestDto,
    currentModel: Model,
    organizationId: string
  ): Promise<ModelUpdateDto> => {
    const updateDto: ModelUpdateDto = {};

    if (request.location) {
      const readModelResult: ReadModelsResponseDto =
        await this.#readModels.execute(
          {
            location: request.location,
          },
          { organizationId }
        );

      if (!readModelResult.success) throw new Error(readModelResult.error);
      if (!readModelResult.value) throw new Error('Reading models failed');
      if (readModelResult.value.length)
        throw new Error(
          `Model with location to be updated is already registered`
        );

      updateDto.location = request.location;
    }
    if (request.parsedLogic) {
      const newModel = Model.create({
        id: request.id,
        location: request.location || currentModel.location,
        parsedLogic: request.parsedLogic,
        lineageId: currentModel.lineageId
      });

      updateDto.logic = newModel.logic;
    }

    return updateDto;
  };
}
