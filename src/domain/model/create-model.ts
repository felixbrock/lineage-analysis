import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Model } from '../entities/model';
import { ParseSQL, ParseSQLResponseDto } from '../sql-parser-api/parse-sql';
// todo cleancode violation
import { ObjectId } from 'mongodb';
import { IModelRepo } from './i-model-repo';
import { ReadModels } from './read-models';

export interface CreateModelRequestDto {
  id: string;
  location: string;
  parsedLogic: string;
  lineageId: string;
}

export interface CreateModelAuthDto {
  organizationId: string;
}

export type CreateModelResponse = Result<Model>;

export class CreateModel
  implements
    IUseCase<CreateModelRequestDto, CreateModelResponse, CreateModelAuthDto>
{

  #readModels: ReadModels;

  #modelRepo: IModelRepo;

  constructor(
    readModels: ReadModels,
    modelRepo: IModelRepo
  ) {
    this.#readModels = readModels;
    this.#modelRepo = modelRepo;
  }

  async execute(
    request: CreateModelRequestDto,
    auth: CreateModelAuthDto
  ): Promise<CreateModelResponse> {
    try {
      const model = Model.create({
        id: new ObjectId().toHexString(),
        location: request.location,
        parsedLogic: request.parsedLogic,
        lineageId: request.lineageId
      });

      const readModelsResult = await this.#readModels.execute(
        {
          location: request.location,
        },
        { organizationId: auth.organizationId }
      );

      if (!readModelsResult.success) throw new Error(readModelsResult.error);
      if (!readModelsResult.value) throw new Error('Reading models failed');
      if (readModelsResult.value.length)
        throw new ReferenceError('Model to be created already exists');

      await this.#modelRepo.insertOne(model);

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      return Result.ok(model);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
