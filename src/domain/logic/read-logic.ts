import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ILogicRepo } from './i-logic-repo';
import { Logic } from '../entities/logic';
import {} from '../services/i-db';

export interface ReadLogicRequestDto {
  id: string;
  targetOrgId?: string;
}

export interface ReadLogicAuthDto {
  callerOrgId?: string;
  isSystemInternal: boolean;
  jwt: string;
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
      const logic = await this.#logicRepo.findOne(
        request.id,
        auth,
        request.targetOrgId
      );
      if (!logic) throw new Error(`Logic with id ${request.id} does not exist`);

      return Result.ok(logic);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
