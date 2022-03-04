import { SQLElement } from "../value-types/sql-element";

export class Table {
  #id: string;

  #name: string;

  #columns: string[];

  #parents: Table[];

  #statementDependencies: [string, string][][];

  get id(): string {
    return this.#id;
  }

  get name(): string {
    return this.#name;
  }

  get columns(): string[] {
    return this.#columns;
  }

  get parents(): Table[] {
    return this.#parents;
  }

  get statementDependencies(): [string, string][][] {
    return this.#statementDependencies;
  }

  static SQLElement = {
    FILE: 'file',
    STATEMENT: 'statement',

    CREATE_TABLE_STATEMENT: 'create_table_statement',

    JOIN_CLAUSE: 'join_clause',
    ODERBY_CLAUSE: 'orderby_clause',

    SELECT_CLAUSE_ELEMENT: 'select_clause_element',
    FROM_EXPRESSION_ELEMENT: 'from_expression_element',
    JOIN_ON_CONDITION: 'join_on_condition',

    TABLE_REFERENCE: 'table_reference',
    COLUMN_REFERENCE: 'column_reference',

    IDENTIFIER: 'identifier',
  };



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

  #populateTableName = (table: Table): void => {
    const tableSelfRef = `${SQLElement.CREATE_TABLE_STATEMENT}.${
      SQLElement.TABLE_REFERENCE
    }.${SQLElement.IDENTIFIER}`;

    const tableSelfSearchRes: string[] = [];
    table.statementDependencies.flat().forEach((element) => {
      if (element.includes(tableSelfRef)) tableSelfSearchRes.push(element[1]);
    });

    if (tableSelfSearchRes.length > 1)
      throw new ReferenceError(`Multiple instances of ${tableSelfRef} found`);
    if (tableSelfSearchRes.length < 1)
      throw new ReferenceError(`${tableSelfRef} not found`);

    this.#name = tableSelfSearchRes[0];
  };

  #populateTableColumns = (table: Table): void => {
    const columnSelfRef = `${SQLElement.SELECT_CLAUSE_ELEMENT}.${
      SQLElement.COLUMN_REFERENCE
    }.${SQLElement.IDENTIFIER}`;

    const columnSelfSearchRes: string[] = [];

    table.statementDependencies.flat().forEach((element) => {
      if (element[0].includes(columnSelfRef))
        columnSelfSearchRes.push(element[1]);
    });

    this.#columns = columnSelfSearchRes;
  };

  #populateParentTableNames = (table: Table): string[] => {
    const tableSelfRef = `${SQLElement.CREATE_TABLE_STATEMENT}.${
      SQLElement.TABLE_REFERENCE
    }.${SQLElement.IDENTIFIER}`;

    const parentTableSearchRes: string[] = [];
    table.statementDependencies.flat().forEach((element) => {
      if (
        !element.includes(tableSelfRef) &&
        element[0].includes(SQLElement.TABLE_REFERENCE)
      )
        parentTableSearchRes.push(element[1]);
    });

    return parentTableSearchRes;
  };

  #populateParents = (): void => {
    const parentTableNames = this.#populateParentTableNames(this);

    // foreach parentnames
      // create table

    console.log(parentTableNames);
  };

  #populateStatementDependencies = (fileObj: any): void => {
    if (
      fileObj.constructor === Object &&
      fileObj[SQLElement.STATEMENT] !== undefined
    ) {
      const statementDependencyObj = this.#extractStatementDependencies(
        SQLElement.IDENTIFIER,
        fileObj[SQLElement.STATEMENT]
      );
      this.statementDependencies.push(statementDependencyObj);
    } else if (Object.prototype.toString.call(fileObj) === '[object Array]') {
      fileObj
        .filter((statement: any) =>
          statement.includes(SQLElement.STATEMENT)
        )
        .forEach((statement: any) => {
          const statementDependencyObj = this.#extractStatementDependencies(
            SQLElement.IDENTIFIER,
            fileObj[SQLElement.STATEMENT]
          );
          this.statementDependencies.push(statementDependencyObj);
        });
    }
  };

  private constructor(id: string) {
    this.#id = id;
    this.#name = '';
    this.#columns = [];
    this.#parents = [];
    this.#statementDependencies = [];
  }

  static create(id: string, parsedSQL: any): Table {
    if (!id) throw new TypeError('Table must have id');

    const table = new Table(id);

    const fileObj = parsedSQL[SQLElement.FILE];

    table.#populateStatementDependencies(fileObj);

    table.#populateTableName(table);

    table.#populateTableColumns(table);

    table.#populateParents();

    return table;
  }
}
