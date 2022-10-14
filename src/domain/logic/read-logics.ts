import { Logic } from '../entities/logic';
import { DbConnection } from '../services/i-db';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { ILogicRepo, LogicQueryDto } from './i-logic-repo';

export interface ReadLogicsRequestDto {
  relationName?: string;
  lineageIds: string[];
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
      if(error instanceof Error && error.message) console.trace(error.message); else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }

  #buildLogicQueryDto = (
    request: ReadLogicsRequestDto,
    organizationId: string
  ): LogicQueryDto => {
    const queryDto: LogicQueryDto = {
      lineageIds: request.lineageIds,
      organizationId,
    };

    if (request.relationName) queryDto.relationName = request.relationName;

    return queryDto;
  };
}
