import IUseCase from '../services/use-case';
import { ModelDto } from './model-dto';
import {
  IModelRepo,
  ModelUpdateDto,
} from './i-model-repo';
import Result from '../value-types/transient-types/result';
import { ReadModels, ReadModelsResponseDto } from './read-models';
import { StatementReference } from '../entities/model';

export interface UpdateModelRequestDto {
  id: string;
  location?: string;
  sql?: string;
  statementReferences?: StatementReference[][];
}

export interface UpdateModelAuthDto {
  organizationId: string;
}

export type UpdateModelResponseDto = Result<ModelDto>;

export class UpdateModel
  implements
    IUseCase<
      UpdateModelRequestDto,
      UpdateModelResponseDto,
      UpdateModelAuthDto
    >
{
  #modelRepo: IModelRepo;

  #readModel: ReadModel;

  #readModels: ReadModels;

  public constructor(
    modelRepo: IModelRepo,
    readModel: ReadModel,
    readModels: ReadModels
  ) {
    this.#modelRepo = modelRepo;
    this.#readModel = readModel;
    this.#readModels = readModels;
  }

  public async execute(
    request: UpdateModelRequestDto,
    auth: UpdateModelAuthDto
  ): Promise<UpdateModelResponseDto> {
    try {
      const readModelResult = await this.#readModel.execute(
        { id: request.id },
        { organizationId: auth.organizationId }
      );

      if (!readModelResult.success)
        throw new Error(readModelResult.error);

      if (!readModelResult.value)
        throw new Error(`Model with id ${request.id} does not exist`);

      // if (readModelResult.value.organizationId !== auth.organizationId)
      //   throw new Error('Not authorized to perform action');

      const updateDto = await this.#buildUpdateDto(
        request,
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
    organizationId: string
  ): Promise<ModelUpdateDto> => {
    const updateDto: ModelUpdateDto = {};

    if (request.content) {
      const readModelResult: ReadModelsResponseDto =
        await this.#readModels.execute(
          {
            content: request.content,
          },
          { organizationId }
        );

      if (!readModelResult.success)
        throw new Error(readModelResult.error);
      if (!readModelResult.value)
        throw new Error('Reading models failed');
      if (readModelResult.value.length)
        throw new Error(
          `Model ${readModelResult.value[0].content} is already registered under ${readModelResult.value[0].id}`
        );

      updateDto.content = request.content;
    }

    if (request.alert) updateDto.alert = Alert.create({});

    updateDto.modifiedOn = Date.now();

    return updateDto;
  };
}
