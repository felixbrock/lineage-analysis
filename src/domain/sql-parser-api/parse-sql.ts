import { URLSearchParams } from 'url';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ISQLParserApiRepo } from './i-sql-parser-api-repo';
import { ParsedSQLDto } from './parsed-sql-dto';

export interface ParseSQLRequestDto {
  dialect: string;
  sql: string;
}

export interface ParseSQLAuthDto {
  jwt: string;
}

export type ParseSQLResponseDto = Result<ParsedSQLDto>;

export class ParseSQL
  implements IUseCase<ParseSQLRequestDto, ParseSQLResponseDto, ParseSQLAuthDto>
{
  #sqlParserApiRepo: ISQLParserApiRepo;

  constructor(sqlParserApiRepo: ISQLParserApiRepo) {
    this.#sqlParserApiRepo = sqlParserApiRepo;
  }

  async execute(
    request: ParseSQLRequestDto,
    auth: ParseSQLAuthDto
  ): Promise<ParseSQLResponseDto> {
    console.log(auth);

    const base64SQL =
      Buffer.from(request.sql, 'base64').toString('base64') === request.sql
        ? request.sql
        : Buffer.from(request.sql).toString('base64');

    try {
      const parseSQLResponse: ParsedSQLDto =
        await this.#sqlParserApiRepo.parseOne(
          new URLSearchParams({ dialect: request.dialect }),
          base64SQL
        );

      if (!parseSQLResponse) throw new Error(`SQL parsing failed`);

      return Result.ok(parseSQLResponse);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
