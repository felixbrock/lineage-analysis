import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
// todo - Clean Code dependency violation. Fix
import { DependencyDto } from './dependency-dto';
import { CreateTable, CreateTableResponseDto } from '../table/create-table';
import { Dependency, DependencyProperties } from '../value-types/dependency';
import { TableDto } from '../table/table-dto';
import { SQLElement } from '../value-types/sql-element';
import { ObjectId } from 'mongodb';
import { Table } from '../entities/table';

export interface CreateDependenciesRequestDto {
  value: string;
  key: string;
  statementReferencesObj: [string, string][];
  parents: Table[];
}

export interface CreateDependenciesAuthDto {
  organizationId: string;
}

export type CreateDependenciesResponseDto = Result<Dependency[]>;

export class CreateDependencies
  implements
    IUseCase<
      CreateDependenciesRequestDto,
      CreateDependenciesResponseDto,
      CreateDependenciesAuthDto
    >
{
  #DependencyElement = {
    NAME: 'name',

    DEPENDENCY_TYPE: 'dependency_type',

    TYPE_SELECT: 'select',
    TYPE_JOIN_CONDITION: 'join_condition',
    TYPE_ORDERBY_CLAUSE: 'oderby_clause',
  };

  // #isColumnDependency = (key: string): boolean =>
  //   !key.includes(SQLElement.INSERT_STATEMENT) &&
  //   key.includes(SQLElement.COLUMN_REFERENCE);

  // #analyzeColumnDependency = (
  //   dependency: [string, string],
  //   dependencyTableName: string,
  //   statementReferencesObj: [string, string][]
  // ) => {
  //   const key = dependency[0];
  //   const value = dependency[1].includes('.')
  //     ? dependency[1].split('.').slice(-1)[0]
  //     : dependency[1];

  //   if (!this.#isColumnDependency(key)) return;

  //   const properties: DependencyProperties = {};

  //   if (key.includes(SQLElement.SELECT_CLAUSE_ELEMENT)) {
  //     result[this.#DependencyElement.TABLE] = dependencyTableName;
  //     result[this.#DependencyElement.TARGETS] = [value];
  //     result[this.#DependencyElement.DEPENDENCY_TYPE] =
  //       this.#DependencyElement.TYPE_SELECT;
  //   } else if (key.includes(SQLElement.JOIN_ON_CONDITION)) {
  //     result[this.#DependencyElement.TABLE] = dependencyTableName;
  //     result[this.#DependencyElement.TARGETS] = [];
  //     result[this.#DependencyElement.DEPENDENCY_TYPE] =
  //       this.#DependencyElement.TYPE_JOIN_CONDITION;
  //   } else if (key.includes(SQLElement.ODERBY_CLAUSE)) {
  //     result[this.#DependencyElement.TABLE] = '';
  //     result[this.#DependencyElement.TARGETS] = [];
  //     result[this.#DependencyElement.DEPENDENCY_TYPE] =
  //       this.#DependencyElement.TYPE_ORDERBY_CLAUSE;
  //   }
  //   result[this.#DependencyElement.COLUMN] = value;
  //   return result;
  // };

  // #findDependencyTable = (
  //   dependency: [string, string],
  //   statementReferences: [string, string][],
  //   parents: TableDto[]
  // ): TableDto => {
  //   const dependencyName = dependency[1].includes('.')
  //     ? dependency[1].split('.').slice(-1)[0]
  //     : dependency[1];
  //   const tableName = dependency[1].includes('.')
  //     ? dependency[1].split('.').slice(0)[0]
  //     : '';

  //   const potentialDependencyTables = parents.filter((table) =>
  //     table.columns.includes(dependencyName)
  //   );

  //   if (potentialDependencyTables.length === 1)
  //     return potentialDependencyTables[0];
  //   else if (potentialDependencyTables.length === 0)
  //     throw new ReferenceError(
  //       `Table for dependency ${dependencyName} not found`
  //     );

  //   if (tableName) {
  //     const dependencyTableMatches = potentialDependencyTables.filter(
  //       (table) => table.name === tableName
  //     );
  //     if (dependencyTableMatches.length === 1) return dependencyTableMatches[0];
  //     throw new ReferenceError('Multiple parents with the same name exist');
  //   }

  //   if (dependency[0].includes(SQLElement.FROM_EXPRESSION_ELEMENT)) {
  //     const potentialMatches = statementReferences.filter((element) =>
  //       [SQLElement.FROM_EXPRESSION_ELEMENT, SQLElement.TABLE_REFERENCE].every(
  //         (key) => element[0].includes(key)
  //       )
  //     );

  //     const dependencyTableMatches = potentialDependencyTables.filter(
  //       (element) =>
  //         potentialMatches.map((match) => match[1]).includes(element.name)
  //     );
  //     if (dependencyTableMatches.length === 1) return dependencyTableMatches[0];
  //     throw new ReferenceError('Multiple parents with the same name exist');
  //   }

  //   throw new ReferenceError(
  //     `Table for dependency ${dependencyName} not found`
  //   );
  // };

  constructor() {}

  async execute(
    request: CreateDependenciesRequestDto,
    auth: CreateDependenciesAuthDto
  ): Promise<CreateDependenciesResponseDto> {
    // Rely on order the statement dependencies were created. Only look at what happenend in statementReferencesObj and what happened before column decleration
    try {
      // const createTableResult: CreateTableResponseDto =
      //   await this.#createTable.execute(
      //     { name: request.name },
      //     { organizationId: 'todo' }
      //   );

      // if (!createTableResult.success) throw new Error(createTableResult.error);
      // if (!createTableResult.value)
      //   throw new Error(`Creation of table ${request.name} failed`);

      // const createTableResults = await Promise.all(
      //   createTableResult.value.parentNames.map(
      //     async (element) =>
      //       await this.#createTable.execute(
      //         { name: element },
      //         { organizationId: 'todo' }
      //       )
      //   )
      // );

      // const parents: TableDto[] = [];
      // createTableResults.forEach((result) => {
      //   if (!result.success) throw new Error(result.error);
      //   if (!result.value) throw new Error(`Creation of parent table failed`);

      //   parents.push(result.value);

      // todo is async ok here?
      //   this.execute({ name: result.value.name }, { organizationId: 'todo' });
      // });

      // const dependency = this.#getDependencies(
      //   createTableResult.value.statementReferences,
      //   createTableResult.value.dependencys,
      //   parents
      // );


      // // The idea was to only go after information occuring before the actual column definition, but it is probably fine to start with as select results 
      // const condition = (element: [string, string]) => element[0] === request.key && element[1] === request.value;

      // const isUnique = request.statementReferencesObj.filter(condition).length === 1;

      // if(!isUnique) throw new ReferenceError('Multiple references for same column found')

      // const index = request.statementReferencesObj.findIndex(condition);

      // const relevantReferences = request.statementReferencesObj.slice();



      // const dependency: { [key: string]: string }[] = [];

      // statementReferences.forEach((statement) => {
      //   statement
      //     .filter((dependency) => this.#isColumnDependency(dependency[0]))
      //     .forEach((dependency) => {
      //       const dependencyTable = this.#findDependencyTable(
      //         dependency,
      //         statement,
      //         parents
      //       );

      //       const result = this.#analyzeColumnDependency(
      //         dependency,
      //         dependencyTable.name,
      //         statement
      //       );

      //       if (!result)
      //         throw new ReferenceError(
      //           'No information for dependency reference found'
      //         );

      //       dependency.push(result);
      //     });
      // });

      // return dependency;

      const dependency = Dependency.create({
        type: 'todo',
        columnId: 'todo'
      });

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      return Result.ok([dependency]);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
