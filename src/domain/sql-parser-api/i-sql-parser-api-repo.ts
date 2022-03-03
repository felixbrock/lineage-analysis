import { URLSearchParams } from "url";
import { ParsedSQLDto } from "./parsed-sql-dto";

export interface ISQLParserApiRepo {
  // todo - add jwt token
  parseOne(params: URLSearchParams, base64SQL: string): Promise<ParsedSQLDto>;
}