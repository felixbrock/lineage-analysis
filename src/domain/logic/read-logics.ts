import { Logic } from '../entities/logic';
import { DbConnection } from '../services/i-db';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { ILogicRepo, LogicQueryDto } from './i-logic-repo';

export interface ReadLogicsRequestDto {
  dbtModelId?: string;
  lineageId: string;
  targetOrganizationId?: string;
}

export interface ReadLogicsAuthDto {
  callerOrganizationId?: string;
  isSystemInternal: boolean;
}

export type ReadLogicsResponseDto = Result<Logic[]>;

export class ReadLogics
  implements
    IUseCase<
      ReadLogicsRequestDto,
      ReadLogicsResponseDto,
      ReadLogicsAuthDto,
      DbConnection
    >
{
  readonly #logicRepo: ILogicRepo;

  #dbConnection: DbConnection;

  constructor(logicRepo: ILogicRepo) {
    this.#logicRepo = logicRepo;
  }

  async execute(
    request: ReadLogicsRequestDto,
    auth: ReadLogicsAuthDto,
    dbConnection: DbConnection
  ): Promise<ReadLogicsResponseDto> {
    try {
      if (auth.isSystemInternal && !request.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrganizationId && !auth.callerOrganizationId)
        throw new Error('No organization Id instance provided');
        if (request.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      let organizationId;
      if (auth.isSystemInternal && request.targetOrganizationId)
        organizationId = request.targetOrganizationId;
      else if (auth.callerOrganizationId)
        organizationId = auth.callerOrganizationId;
      else throw new Error('Unhandled organizationId allocation');

      this.#dbConnection = dbConnection;

      const logics: Logic[] = await this.#logicRepo.findBy(
        this.#buildLogicQueryDto(request, organizationId),
        this.#dbConnection
      );
      if (!logics) throw new Error(`Queried logics do not exist`);

      return Result.ok(logics);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.stack || error.message);
      return Result.fail('Unknown error occured');
    }
  }

  #buildLogicQueryDto = (
    request: ReadLogicsRequestDto,
    organizationId: string
  ): LogicQueryDto => {
    const queryDto: LogicQueryDto = {
      lineageId: request.lineageId,
      organizationId,
    };

    if (request.dbtModelId) queryDto.dbtModelId = request.dbtModelId;

    return queryDto;
  };
}
