import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ILogicRepo } from './i-logic-repo';
import { Logic } from '../entities/logic';
import { DbConnection } from '../services/i-db';

export interface ReadLogicRequestDto {
  id: string;
}

export interface ReadLogicAuthDto {
  callerOrganizationId: string;
}

export type ReadLogicResponseDto = Result<Logic>;

export class ReadLogic
  implements
    IUseCase<
      ReadLogicRequestDto,
      ReadLogicResponseDto,
      ReadLogicAuthDto,
      DbConnection
    >
{
  readonly #logicRepo: ILogicRepo;

  #dbConnection: DbConnection;

  constructor(logicRepo: ILogicRepo) {
    this.#logicRepo = logicRepo;
  }

  async execute(
    request: ReadLogicRequestDto,
    auth: ReadLogicAuthDto,
    dbConnection: DbConnection
  ): Promise<ReadLogicResponseDto> {
    try {

      this.#dbConnection = dbConnection;

      const logic = await this.#logicRepo.findOne(
        request.id,
        this.#dbConnection
      );
      if (!logic) throw new Error(`Logic with id ${request.id} does not exist`);

      if (logic.organizationId !== auth.callerOrganizationId)
        throw new Error('Not authorized to perform action');

      return Result.ok(logic);
    } catch (error: unknown) {
      if(error instanceof Error && error.message) console.trace(error.message); else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
