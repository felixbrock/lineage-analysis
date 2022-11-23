import { URLSearchParams } from 'url';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ISQLParserApiRepo } from './i-sql-parser-api-repo';
import { ParsedSQLDto } from './parsed-sql-dto';

export interface ParseSQLRequestDto {
  dialect: string;
  sql: string;
}

export type ParseSQLAuthDto = null;

export type ParseSQLResponseDto = Result<ParsedSQLDto>;

export class ParseSQL
  implements IUseCase<ParseSQLRequestDto, ParseSQLResponseDto, ParseSQLAuthDto>
{
  readonly #sqlParserApiRepo: ISQLParserApiRepo;

  constructor(sqlParserApiRepo: ISQLParserApiRepo) {
    this.#sqlParserApiRepo = sqlParserApiRepo;
  }

  async execute(request: ParseSQLRequestDto): Promise<ParseSQLResponseDto> {
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
      if(error instanceof Error && error.message) console.error(error.stack); else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
