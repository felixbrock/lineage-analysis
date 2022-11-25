import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ILogicRepo } from './i-logic-repo';
import { Logic } from '../entities/logic';
 
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import BaseAuth from '../services/base-auth';

export interface ReadLogicRequestDto {
  id: string;
  targetOrgId?: string;
  
}

export type ReadLogicAuthDto = BaseAuth;

export type ReadLogicResponseDto = Result<Logic>;

export class ReadLogic
  implements
    IUseCase<ReadLogicRequestDto, ReadLogicResponseDto, ReadLogicAuthDto,IConnectionPool>
{
  readonly #logicRepo: ILogicRepo;

  constructor(logicRepo: ILogicRepo) {
    this.#logicRepo = logicRepo;
  }

  async execute(
    req: ReadLogicRequestDto,
    auth: ReadLogicAuthDto,
    connPool: IConnectionPool
  ): Promise<ReadLogicResponseDto> {
    try {
      const logic = await this.#logicRepo.findOne(
        req.id,
        auth,
        connPool,
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
