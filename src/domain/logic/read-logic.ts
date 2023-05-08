import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ILogicRepo } from './i-logic-repo';
import { Logic } from '../entities/logic';
 
import BaseAuth from '../services/base-auth';
import { IDbConnection } from '../services/i-db';

export interface ReadLogicRequestDto {
  id: string;
  targetOrgId?: string;
  
}

export type ReadLogicAuthDto = BaseAuth;

export type ReadLogicResponseDto = Result<Logic>;

export class ReadLogic
  implements
    IUseCase<ReadLogicRequestDto, ReadLogicResponseDto, ReadLogicAuthDto, IDbConnection>
{
  readonly #logicRepo: ILogicRepo;

  constructor(logicRepo: ILogicRepo) {
    this.#logicRepo = logicRepo;
  }

  async execute(
    req: ReadLogicRequestDto,
    auth: ReadLogicAuthDto,
    dbConnection: IDbConnection
  ): Promise<ReadLogicResponseDto> {
    try {
      const logic = await this.#logicRepo.findOne(
        req.id,
        auth,
        dbConnection,
      );
      if (!logic) throw new Error(`Logic with id ${req.id} does not exist`);

      return Result.ok(logic);
    } catch (error: unknown) {
      if (error instanceof Error ) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
