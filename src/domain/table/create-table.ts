import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { TableDto } from './table-dto';
// todo - Clean Code dependency violation. Fix
import fs from 'fs';
import { Table } from '../entities/table';
import { ParseSQL } from '../sql-parser-api/parse-sql';
import { SQLElement } from '../value-types/sql-element';
// todo cleancode violation
import { ObjectId } from 'mongodb';

export interface CreateTableRequestDto {
  name: string;
}

export interface CreateTableAuthDto {
  organizationId: string;
}

export type CreateTableResponseDto = Result<TableDto>;

export class CreateTable
  implements
    IUseCase<CreateTableRequestDto, CreateTableResponseDto, CreateTableAuthDto>
{
  #parseSQL: ParseSQL;

  constructor(parseSQL: ParseSQL) {
    this.#parseSQL = parseSQL;
  }

  #appendPath = (key: string, path: string): string => {
    let newPath = path;
    newPath += !path ? key : `.${key}`;
    return newPath;
  };

  #extractStatementDependencies = (
    targetKey: string,
    parsedSQL: { [key: string]: any },
    path = ''
  ): [string, string][] => {
    const statementDependencyObj: [string, string][] = [];

    Object.entries(parsedSQL).forEach((element) => {
      const key = element[0];
      const value = element[1];

      if (key === targetKey)
        statementDependencyObj.push([this.#appendPath(key, path), value]);

      // check if value is dictionary
      if (value.constructor === Object) {
        const dependencies = this.#extractStatementDependencies(
          targetKey,
          value,
          this.#appendPath(key, path)
        );
        dependencies.forEach((dependencyElement) =>
          statementDependencyObj.push(dependencyElement)
        );
      } else if (Object.prototype.toString.call(value) === '[object Array]') {
        if (key === SQLElement.COLUMN_REFERENCE) {
          let valuePath = '';
          let keyPath = '';
          value.forEach((valueElement: { [key: string]: any }) => {
            const dependencies = this.#extractStatementDependencies(
              targetKey,
              valueElement,
              this.#appendPath(key, path)
            );
            dependencies.forEach((dependencyElement: [string, string]) => {
              valuePath = this.#appendPath(dependencyElement[1], valuePath);
              [keyPath] = dependencyElement;
            });
          });
          statementDependencyObj.push([keyPath, valuePath]);
        } else {
          value.forEach((valueElement: { [key: string]: any }) => {
            const dependencies = this.#extractStatementDependencies(
              targetKey,
              valueElement,
              this.#appendPath(key, path)
            );
            dependencies.forEach((dependencyElement) =>
              statementDependencyObj.push(dependencyElement)
            );
          });
        }
      }
    });

    return statementDependencyObj;
  };

  #getTableName = (statementDependencies: [string, string][][]): string => {
    const tableSelfRef = `${SQLElement.CREATE_TABLE_STATEMENT}.${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`;

    const tableSelfSearchRes: string[] = [];
    statementDependencies.flat().forEach((element) => {
      if (element.includes(tableSelfRef)) tableSelfSearchRes.push(element[1]);
    });

    if (tableSelfSearchRes.length > 1)
      throw new ReferenceError(`Multiple instances of ${tableSelfRef} found`);
    if (tableSelfSearchRes.length < 1)
      throw new ReferenceError(`${tableSelfRef} not found`);

    return tableSelfSearchRes[0];
  };

  #getTableColumns = (
    statementDependencies: [string, string][][]
  ): string[] => {
    const columnSelfRefs = [
      `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.COLUMN_REFERENCE}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.COLUMN_DEFINITION}.${SQLElement.IDENTIFIER}`,
    ];

    const columnSelfSearchRes: string[] = [];

    statementDependencies.flat().forEach((element) => {
      if (columnSelfRefs.some((ref) => element[0].includes(ref)))
        columnSelfSearchRes.push(element[1]);
    });

    return columnSelfSearchRes;
  };

  #getParentTableNames = (
    statementDependencies: [string, string][][]
  ): string[] => {
    const tableRef = `${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`;
    const tableSelfRefs = [
      `${SQLElement.CREATE_TABLE_STATEMENT}.${tableRef}`,
      `${SQLElement.INSERT_STATEMENT}.${tableRef}`,
    ];

    const parentTableNames: string[] = [];
    statementDependencies.flat().forEach((element) => {
      if (
        !tableSelfRefs.some((ref) => element.includes(ref)) &&
        element[0].includes(SQLElement.TABLE_REFERENCE)
      )
        parentTableNames.push(element[1]);
    });

    return parentTableNames;
  };

  // #populateParents = (): void => {
  //   const parentTableNames = this.#getParentTableNames(this);

  //   parentTableNames.forEach(element => {

  //   })

  //   console.log(parentTableNames);
  // };

  #getStatementDependencies = (fileObj: any): [string, string][][] => {
    const statementDependencies: [string, string][][] = [];

    if (
      fileObj.constructor === Object &&
      fileObj[SQLElement.STATEMENT] !== undefined
    ) {
      const statementDependencyObj = this.#extractStatementDependencies(
        SQLElement.IDENTIFIER,
        fileObj[SQLElement.STATEMENT]
      );
      statementDependencies.push(statementDependencyObj);
    } else if (Object.prototype.toString.call(fileObj) === '[object Array]') {
      fileObj
        .filter((statement: any) => SQLElement.STATEMENT in statement)
        .forEach((statement: any) => {
          const statementDependencyObj = this.#extractStatementDependencies(
            SQLElement.IDENTIFIER,
            statement[SQLElement.STATEMENT]
          );
          statementDependencies.push(statementDependencyObj);
        });
    }

    return statementDependencies;
  };

  async execute(
    request: CreateTableRequestDto,
    auth: CreateTableAuthDto
  ): Promise<CreateTableResponseDto> {
    try {
      const data = fs.readFileSync(
        `C://Users/felix-pc/Desktop/Test/${request.name}.sql`,
        'base64'
      );

      const parseSQLResult: Result<any> = await this.#parseSQL.execute(
        { dialect: 'snowflake', sql: data },
        { jwt: 'XXX' }
      );

      if (!parseSQLResult.success) throw new Error(parseSQLResult.error);
      if (!parseSQLResult.value) throw new Error(`Parsing of SQL logic failed`);

      const statementDependencies = this.#getStatementDependencies(
        parseSQLResult.value[SQLElement.FILE]
      );

      const name = this.#getTableName(statementDependencies);

      const columns = this.#getTableColumns(statementDependencies);

      const parentNames = this.#getParentTableNames(statementDependencies);

      const table = Table.create({
        id: new ObjectId().toHexString(),
        name,
        columns,
        parentNames,
        statementDependencies,
      });

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      return Result.ok({
        id: table.id,
        name: table.name,
        columns: table.columns,
        parentNames: table.parentNames,
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
