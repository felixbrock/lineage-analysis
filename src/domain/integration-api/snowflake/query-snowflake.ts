import Result from '../../value-types/transient-types/result';
import IUseCase from '../../services/use-case';
import {
  IIntegrationApiRepo,
  SnowflakeQueryResultDto,
} from '../i-integration-api-repo';

export interface QuerySnowflakeRequestDto {
  query: string;
  targetOrganizationId?: string;
}

export interface QuerySnowflakeAuthDto {
  jwt: string;
}

export type QuerySnowflakeResponseDto = Result<SnowflakeQueryResultDto>;

export class QuerySnowflake
  implements
    IUseCase<
      QuerySnowflakeRequestDto,
      QuerySnowflakeResponseDto,
      QuerySnowflakeAuthDto
    >
{
  readonly #integrationApiRepo: IIntegrationApiRepo;

  constructor(integrationApiRepo: IIntegrationApiRepo) {
    this.#integrationApiRepo = integrationApiRepo;
  }

  async execute(
    request: QuerySnowflakeRequestDto,
    auth: QuerySnowflakeAuthDto
  ): Promise<QuerySnowflakeResponseDto> {
    try {
      const querySnowflakeResponse: SnowflakeQueryResultDto =
        await this.#integrationApiRepo.querySnowflake(request, auth.jwt);

      return Result.ok(querySnowflakeResponse);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
