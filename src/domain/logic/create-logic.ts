// todo clean architecture violation
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Logic } from '../entities/logic';
import { ILogicRepo } from './i-logic-repo';
import { ReadLogics } from './read-logics';

export interface CreateLogicRequestDto {
  dbtModelId: string;
  parsedLogic: string;
  lineageId: string;
}

export interface CreateLogicAuthDto {
  organizationId: string;
}

export type CreateLogicResponse = Result<Logic>;

export class CreateLogic
  implements
    IUseCase<CreateLogicRequestDto, CreateLogicResponse, CreateLogicAuthDto>
{
  readonly #readLogics: ReadLogics;

  readonly #logicRepo: ILogicRepo;

  constructor(readLogics: ReadLogics, logicRepo: ILogicRepo) {
    this.#readLogics = readLogics;
    this.#logicRepo = logicRepo;
  }

  async execute(
    request: CreateLogicRequestDto,
    auth: CreateLogicAuthDto
  ): Promise<CreateLogicResponse> {
    try {
      const logic = Logic.create({
        id: new ObjectId().toHexString(),
        dbtModelId: request.dbtModelId,
        parsedLogic: request.parsedLogic,
        lineageId: request.lineageId,
      });

      const readLogicsResult = await this.#readLogics.execute(
        {
          dbtModelId: request.dbtModelId,
          lineageId: request.lineageId,
        },
        { organizationId: auth.organizationId }
      );

      if (!readLogicsResult.success) throw new Error(readLogicsResult.error);
      if (!readLogicsResult.value) throw new Error('Reading logics failed');
      if (readLogicsResult.value.length)
        throw new ReferenceError('Logic to be created already exists');

      await this.#logicRepo.insertOne(logic);

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      return Result.ok(logic);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
