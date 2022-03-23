import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
// todo - Clean Code dependency violation. Fix
import fs from 'fs';
import { Model, StatementReference } from '../entities/model';
import { ParseSQL, ParseSQLResponseDto } from '../sql-parser-api/parse-sql';
import { SQLElement } from '../value-types/sql-element';
// todo cleancode violation
import { ObjectId } from 'mongodb';

export interface CreateModelRequestDto {
  id: string;
}

export interface CreateModelAuthDto {
  organizationId: string;
}

export type CreateModelResponse = Result<Model>;

export class CreateModel
  implements
    IUseCase<CreateModelRequestDto, CreateModelResponse, CreateModelAuthDto>
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

  #extractstatementReferences = (
    targetKey: string,
    parsedSQL: { [key: string]: any },
    path = ''
  ): StatementReference[] => {
    const statementReferencesObj: StatementReference[] = [];

    Object.entries(parsedSQL).forEach((element) => {
      const key = element[0];
      const value = element[1];

      if (key === targetKey)
        statementReferencesObj.push([this.#appendPath(key, path), value]);
      else if (key === SQLElement.KEYWORD && value === SQLElement.KEYWORD_AS)
        path = this.#appendPath(value, path);

      // check if value is dictionary
      if (value.constructor === Object) {
        const dependencies = this.#extractstatementReferences(
          targetKey,
          value,
          this.#appendPath(key, path)
        );
        dependencies.forEach((dependencyElement) =>
          statementReferencesObj.push(dependencyElement)
        );
      } else if (Object.prototype.toString.call(value) === '[object Array]') {
        if (key === SQLElement.COLUMN_REFERENCE) {
          let valuePath = '';
          let keyPath = '';
          value.forEach((valueElement: { [key: string]: any }) => {
            const dependencies = this.#extractstatementReferences(
              targetKey,
              valueElement,
              this.#appendPath(key, path)
            );
            dependencies.forEach((dependencyElement: [string, string]) => {
              valuePath = this.#appendPath(dependencyElement[1], valuePath);
              [keyPath] = dependencyElement;
            });
          });
          statementReferencesObj.push([keyPath, valuePath]);
        } else {
          value.forEach((valueElement: { [key: string]: any }) => {
            const dependencies = this.#extractstatementReferences(
              targetKey,
              valueElement,
              this.#appendPath(key, path)
            );
            dependencies.forEach((dependencyElement) =>
              statementReferencesObj.push(dependencyElement)
            );
          });
        }
      }
    });

    return statementReferencesObj;
  };

  #getstatementReferences = (fileObj: any): StatementReference[][] => {
    const statementReferences: StatementReference[][] = [];

    if (
      fileObj.constructor === Object &&
      fileObj[SQLElement.STATEMENT] !== undefined
    ) {
      const statementReferencesObj = this.#extractstatementReferences(
        SQLElement.IDENTIFIER,
        fileObj[SQLElement.STATEMENT]
      );
      statementReferences.push(statementReferencesObj);
    } else if (Object.prototype.toString.call(fileObj) === '[object Array]') {
      fileObj
        .filter((statement: any) => SQLElement.STATEMENT in statement)
        .forEach((statement: any) => {
          const statementReferencesObj = this.#extractstatementReferences(
            SQLElement.IDENTIFIER,
            statement[SQLElement.STATEMENT]
          );
          statementReferences.push(statementReferencesObj);
        });
    }

    return statementReferences;
  };

  async execute(
    request: CreateModelRequestDto,
    auth: CreateModelAuthDto
  ): Promise<CreateModelResponse> {
    try {
      const data = fs.readFileSync(
        `C://Users/felix-pc/Desktop/Test/${request.id}.sql`,
        'base64'
      );

      const parseSQLResult: ParseSQLResponseDto = await this.#parseSQL.execute(
        { dialect: 'snowflake', sql: data },
        { jwt: 'XXX' }
      );

      if (!parseSQLResult.success) throw new Error(parseSQLResult.error);
      if (!parseSQLResult.value) throw new SyntaxError(`Parsing of SQL logic failed`);

      const statementReferences = this.#getstatementReferences(
        parseSQLResult.value.file
      );

      const model = Model.create({
        id: new ObjectId().toHexString(),
        sql: parseSQLResult.value.file,
        statementReferences,
      });

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      return Result.ok(model);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
