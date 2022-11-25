import { Logic } from '../entities/logic';
import BaseAuth from '../services/base-auth';
 
import IUseCase from '../services/use-case';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import Result from '../value-types/transient-types/result';
import { ILogicRepo, LogicQueryDto } from './i-logic-repo';

export interface ReadLogicsRequestDto {
  relationName?: string;
  lineageId: string;
  targetOrgId?: string;
}

export type ReadLogicsAuthDto = BaseAuth;

export type ReadLogicsResponseDto = Result<Logic[]>;

export class ReadLogics
  implements
    IUseCase<ReadLogicsRequestDto, ReadLogicsResponseDto, ReadLogicsAuthDto,IConnectionPool>
{
  readonly #logicRepo: ILogicRepo;

  constructor(logicRepo: ILogicRepo) {
    this.#logicRepo = logicRepo;
  }

  async execute(
    req: ReadLogicsRequestDto,
    auth: ReadLogicsAuthDto,
    connPool: IConnectionPool
  ): Promise<ReadLogicsResponseDto> {
    try {
      if (auth.isSystemInternal && !req.targetOrgId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrgId)
        throw new Error('Caller organization id missing');
      if (!req.targetOrgId && !auth.callerOrgId)
        throw new Error('No organization Id instance provided');
      if (req.targetOrgId && auth.callerOrgId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const logics: Logic[] = await this.#logicRepo.findBy(
        this.#buildLogicQueryDto(req),
        auth,
        connPool,
      );
      if (!logics) throw new Error(`Queried logics do not exist`);

      return Result.ok(logics);
    } catch (error: unknown) {
      if (error instanceof Error ) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }

  #buildLogicQueryDto = (request: ReadLogicsRequestDto): LogicQueryDto => {
    const queryDto: LogicQueryDto = {
      lineageId: request.lineageId,
    };

    if (request.relationName) queryDto.relationName = request.relationName;

    return queryDto;
  };
}
