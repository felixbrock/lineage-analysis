import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { TableDto } from './table-dto';
// todo - Clean Code dependency violation. Fix
import fs from 'fs';
import { Table } from '../entities/table';
import { ParseSQL } from '../sql-parser-api/parse-sql';

export interface ReadTableRequestDto {
  id: string;
}

export interface ReadTableAuthDto {
  organizationId: string;
}

export type ReadTableResponseDto = Result<TableDto>;

export class ReadTable
  implements
    IUseCase<ReadTableRequestDto, ReadTableResponseDto, ReadTableAuthDto>
{
  #parseSQL: ParseSQL;

  constructor(parseSQL: ParseSQL) {
    this.#parseSQL = parseSQL;
  }

  async execute(
    request: ReadTableRequestDto,
    auth: ReadTableAuthDto
  ): Promise<ReadTableResponseDto> {
    try {
      const data = fs.readFileSync(
        'C://Users/felix-pc/Desktop/Test/table2.sql',
        'base64'
      );

      const parseSQLResult: Result<any> = await this.#parseSQL.execute(
        { dialect: 'snowflake', sql: data },
        { jwt: 'XXX' }
      );

      if (!parseSQLResult.success) throw new Error(parseSQLResult.error);

      const table = Table.create(request.id, parseSQLResult.value);

      console.log(table.name);
      console.log(table.columns);
      console.log(table.statementDependencies);

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      return Result.ok({
        id: table.id,
        name: table.name,
        columns: table.columns,
        parents: table.parents,
        statementDependencies: table.statementDependencies,
      });
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }

  // #runChildProcess = () => {
  //   const childProcess = spawn('python', [
  //     '../value-types/sql-parser.py',
  //     // id,
  //     'C://Users/felix-pc/Desktop/Test/table2.sql',
  //     'snowflake',
  //   ]);

  //       const processResults: any[] = [];

  //       childProcess.stdout.on('data', (data) =>
  //         processResults.push(data.toString())
  //       );

  //       childProcess.on('close', (code) => {
  // });}
}
