import { URLSearchParams } from "url";
import { ParsedSQLDto } from "./parsed-sql-dto";

export interface ISQLParserApiRepo {
  parseOne(params: URLSearchParams, base64SQL: string): Promise<ParsedSQLDto>;
}