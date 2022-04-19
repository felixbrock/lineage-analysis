import { Logic } from '../entities/logic';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { ILogicRepo, LogicQueryDto } from './i-logic-repo';

export interface ReadLogicsRequestDto {
  dbtModelId?: string;
  lineageId: string;
}

export interface ReadLogicsAuthDto {
  organizationId: string;
}

export type ReadLogicsResponseDto = Result<Logic[]>;

export class ReadLogics
  implements
    IUseCase<ReadLogicsRequestDto, ReadLogicsResponseDto, ReadLogicsAuthDto>
{
  readonly #logicRepo: ILogicRepo;

  constructor(logicRepo: ILogicRepo) {
    this.#logicRepo = logicRepo;
  }

  async execute(
    request: ReadLogicsRequestDto,
    auth: ReadLogicsAuthDto
  ): Promise<ReadLogicsResponseDto> {
    try {
      const logics: Logic[] = await this.#logicRepo.findBy(
        this.#buildLogicQueryDto(request, auth.organizationId)
      );
      if (!logics) throw new Error(`Queried logics do not exist`);

      return Result.ok(logics);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }

  #buildLogicQueryDto = (
    request: ReadLogicsRequestDto,
    organizationId: string
  ): LogicQueryDto => {
    console.log(organizationId);

    const queryDto: LogicQueryDto = { lineageId: request.lineageId };

    // todo - add organizationId
    // queryDto.organizationId = organizationId;
    if (request.dbtModelId) queryDto.dbtModelId = request.dbtModelId;

    return queryDto;
  };
}