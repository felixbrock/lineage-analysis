import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ILogicRepo } from './i-logic-repo';
import { Logic } from '../entities/logic';

export interface ReadLogicRequestDto {
  id: string;
}

export interface ReadLogicAuthDto {
  organizationId: string;
}

export type ReadLogicResponseDto = Result<Logic>;

export class ReadLogic
  implements
    IUseCase<ReadLogicRequestDto, ReadLogicResponseDto, ReadLogicAuthDto>
{
  readonly #logicRepo: ILogicRepo;

  constructor(logicRepo: ILogicRepo) {
    this.#logicRepo = logicRepo;
  }

  async execute(
    request: ReadLogicRequestDto,
    auth: ReadLogicAuthDto
  ): Promise<ReadLogicResponseDto> {
    try {
      // todo -replace
      console.log(auth);

      const logic = await this.#logicRepo.findOne(request.id);
      if (!logic) throw new Error(`Logic with id ${request.id} does not exist`);

      // if (logic.organizationId !== auth.organizationId)
      //   throw new Error('Not authorized to perform action');

      return Result.ok(logic);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}