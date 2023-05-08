import { Logic } from '../entities/logic';
import BaseAuth from '../services/base-auth';
import { IDbConnection } from '../services/i-db';
 
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { ILogicRepo, LogicQueryDto } from './i-logic-repo';

export interface ReadLogicsRequestDto {
  relationNames?: string[];
  targetOrgId?: string;
}

export type ReadLogicsAuthDto = BaseAuth;

export type ReadLogicsResponseDto = Result<Logic[]>;

export class ReadLogics
  implements
    IUseCase<ReadLogicsRequestDto, ReadLogicsResponseDto, ReadLogicsAuthDto, IDbConnection>
{
  readonly #logicRepo: ILogicRepo;

  constructor(logicRepo: ILogicRepo) {
    this.#logicRepo = logicRepo;
  }

  async execute(
    req: ReadLogicsRequestDto,
    auth: ReadLogicsAuthDto,
    dbConnection: IDbConnection
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
        dbConnection,
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
    };

    if (request.relationNames) queryDto.relationNames = request.relationNames;

    return queryDto;
  };
}
