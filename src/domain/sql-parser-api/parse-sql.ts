import { URLSearchParams } from 'url';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ISQLParserApiRepo } from './i-sql-parser-api-repo';
import { ParsedSQLDto } from './parsed-sql-dto';

export interface ParseSQLRequestDto {
  dialect: string;
  sql: string
}

export interface ParseSQLAuthDto {
  jwt: string;
}

export type ParseSQLResponseDto = Result<ParsedSQLDto>;

export class ParseSQL
  implements
    IUseCase<ParseSQLRequestDto, ParseSQLResponseDto, ParseSQLAuthDto>
{
  #sqlParserApiRepo: ISQLParserApiRepo;

  constructor(sqlParserApiRepo: ISQLParserApiRepo) {
    this.#sqlParserApiRepo = sqlParserApiRepo;
  }

  async execute(
    request: ParseSQLRequestDto,
    auth: ParseSQLAuthDto
  ): Promise<ParseSQLResponseDto> {
    try {

    //   let base64SQL; 
    //   try {
    //     Buffer
    //     atob(request));
    // } catch(e) {
    //     console.log(e)
    //     // something failed
    
    //     // if you want to be specific and only catch the error which means
    //     // the base 64 was invalid, then check for 'e.code === 5'.
    //     // (because 'DOMException.INVALID_CHARACTER_ERR === 5')
    // }


      const parseSQLResponse: ParsedSQLDto =
        await this.#sqlParserApiRepo.parseOne(
          new URLSearchParams({ dialect: request.dialect}),
          request.sql
        );

      if (!parseSQLResponse)
        throw new Error(`SQL parsing failed`);

      return Result.ok(parseSQLResponse);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}