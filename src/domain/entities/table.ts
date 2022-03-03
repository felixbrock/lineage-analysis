export class Table {
  #id: string;

  #name: string;

  #columns: string[];

  #parents: Table[];

  #statementDependencies: [string, string][][];

  #lineageInfo: { [key: string]: string }[];

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

  get lineageInfo(): { [key: string]: string }[] {
    return this.#lineageInfo;
  }

  #SQLElement = {
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

  #LineageInfo = {
    TABLE_SELF: 'table_self',
    TABLE: 'table',

    COLUMN: 'column',

    DEPENDENCY_TYPE: 'dependency_type',

    TYPE_SELECT: 'select',
    TYPE_JOIN_CONDITION: 'join_condition',
    TYPE_ORDERBY_CLAUSE: 'oderby_clause',
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
        if (key === this.#SQLElement.COLUMN_REFERENCE) {
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

  #isColumnDependency = (key: string): boolean =>
    key.includes(this.#SQLElement.COLUMN_REFERENCE);

  #populateTableName = (table: Table): void => {
    const tableSelfRef = `${this.#SQLElement.CREATE_TABLE_STATEMENT}.${
      this.#SQLElement.TABLE_REFERENCE
    }.${this.#SQLElement.IDENTIFIER}`;

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
    const columnSelfRef = `${this.#SQLElement.SELECT_CLAUSE_ELEMENT}.${
      this.#SQLElement.COLUMN_REFERENCE
    }.${this.#SQLElement.IDENTIFIER}`;

    const columnSelfSearchRes: string[] = [];

    table.statementDependencies.flat().forEach((element) => {
      if (element[0].includes(columnSelfRef))
        columnSelfSearchRes.push(element[1]);
    });

    this.#columns = columnSelfSearchRes;
  };

  #populateParentTableNames = (table: Table): string[] => {
    const tableSelfRef = `${this.#SQLElement.CREATE_TABLE_STATEMENT}.${
      this.#SQLElement.TABLE_REFERENCE
    }.${this.#SQLElement.IDENTIFIER}`;

    const parentTableSearchRes: string[] = [];
    table.statementDependencies.flat().forEach((element) => {
      if (
        !element.includes(tableSelfRef) &&
        element[0].includes(this.#SQLElement.TABLE_REFERENCE)
      )
        parentTableSearchRes.push(element[1]);
    });

    return parentTableSearchRes;
  };

  #analyzeColumnDependency = (
    table: Table,
    key: string,
    value: string,
    dependencyObjIndex: number
  ) => {
    if (!this.#isColumnDependency(key)) return;

    const result: { [key: string]: string } = {};
    let tableRef = '';
    let valueRef = value;

    if (value.includes('.')) {
      const valuePathElements = value.split('.');
      tableRef = valuePathElements[0];
      valueRef = valuePathElements[1];
    }

    const statementDependencyObj =
      table.statementDependencies[dependencyObjIndex];

    if (key.includes(this.#SQLElement.SELECT_CLAUSE_ELEMENT)) {
      if (!tableRef) {
        const tableRefs = statementDependencyObj.filter((element) => 
          [
            this.#SQLElement.FROM_EXPRESSION_ELEMENT,
            this.#SQLElement.TABLE_REFERENCE,
          ].every((substring) => element[0].includes(substring))        );
        tableRef = tableRefs[0][1]
      }

      if (!tableRef)
        throw ReferenceError(`No table for SELECT statement found`);

      result[this.#LineageInfo.TABLE] = tableRef;
      result[this.#LineageInfo.DEPENDENCY_TYPE] = this.#LineageInfo.TYPE_SELECT;
    } else if (key.includes(this.#SQLElement.JOIN_ON_CONDITION)) {
      if (!tableRef) {
        Object.entries(statementDependencyObj).forEach((element) => {
          const isJoinTable = [
            this.#SQLElement.JOIN_CLAUSE,
            this.#SQLElement.FROM_EXPRESSION_ELEMENT,
            this.#SQLElement.TABLE_REFERENCE,
          ].every((substring) => element[0].includes(substring));
          if (isJoinTable) {
            tableRef = value;
            return;
          }
        });

        if (!tableRef)
          throw ReferenceError(`No table for JOIN statement found`);
      }

      result[this.#LineageInfo.TABLE] = tableRef;
      result[this.#LineageInfo.DEPENDENCY_TYPE] =
        this.#LineageInfo.TYPE_JOIN_CONDITION;
    } else if (key.includes(this.#SQLElement.ODERBY_CLAUSE))
      result[this.#LineageInfo.DEPENDENCY_TYPE] =
        this.#LineageInfo.TYPE_ORDERBY_CLAUSE;

    result[this.#LineageInfo.COLUMN] = valueRef;
    return result;
  };

  #populateParents = (): void => {
    const parentTableNames = this.#populateParentTableNames(this);
    console.log(parentTableNames);
  };

  #populateStatementDependencies = (fileObj: any): void => {
    if (
      fileObj.constructor === Object &&
      fileObj[this.#SQLElement.STATEMENT] !== undefined
    ) {
      const statementDependencyObj = this.#extractStatementDependencies(
        this.#SQLElement.IDENTIFIER,
        fileObj[this.#SQLElement.STATEMENT]
      );
      this.statementDependencies.push(statementDependencyObj);
    } else if (Object.prototype.toString.call(fileObj) === '[object Array]') {
      fileObj
        .filter((statement: any) =>
          statement.includes(this.#SQLElement.STATEMENT)
        )
        .forEach((statement: any) => {
          const statementDependencyObj = this.#extractStatementDependencies(
            this.#SQLElement.IDENTIFIER,
            fileObj[this.#SQLElement.STATEMENT]
          );
          this.statementDependencies.push(statementDependencyObj);
        });
    }
  };

  #populateLineageInfo = () => {
    let counter = 0;
    this.#statementDependencies.forEach((element) => {
      element
        .filter((dependency) => this.#isColumnDependency(dependency[0]))
        .forEach((dependency) => {
          const result = this.#analyzeColumnDependency(
            this,
            dependency[0],
            dependency[1],
            counter
          );

          if (!result)
            throw new ReferenceError(
              'No information for column reference found'
            );

          this.#lineageInfo.push(result);
        });
      counter += 1;
    });
  };

  private constructor(id: string) {
    this.#id = id;
    this.#name = '';
    this.#columns = [];
    this.#parents = [];
    this.#statementDependencies = [];
    this.#lineageInfo = [];
  }

  static create(id: string, parsedSQL: any): Table {
    if (!id) throw new TypeError('Table must have id');

    const table = new Table(id);

    const fileObj = parsedSQL[table.#SQLElement.FILE];

    table.#populateStatementDependencies(fileObj);

    table.#populateTableName(table);

    table.#populateTableColumns(table);

    table.#populateParents();

    table.#populateLineageInfo();

    // #     # TODO - resolve analysis result to properties
    return table;
  }
}
