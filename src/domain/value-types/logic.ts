import { DependencyType } from '../entities/dependency';
import SQLElement from './sql-element';

interface Ref {
  path: string;
  name: string;
  alias?: string;
  schemaName?: string;
  databaseName?: string;
  warehouseName?: string;
}

export interface TableRef extends Ref {
  isSelfRef: boolean;
}

export interface ColumnRef extends Ref {
  dependencyType: DependencyType;
  isWildcardRef: boolean;
  tableName?: string;
}

export interface Refs {
  tables: TableRef[];
  columns: ColumnRef[];
  wildcards: ColumnRef[];
}

interface LogicProperties {
  parsedLogic: string;
  statementRefs: Refs[];
}

export interface LogicPrototype {
  parsedLogic: string;
}

interface HandlerProperties<ValueType> {
  key: string;
  value: ValueType;
  alias?: string;
  refPath: string;
}

export class Logic {
  #parsedLogic: string;

  #statementRefs: Refs[];

  get parsedLogic(): string {
    return this.#parsedLogic;
  }

  get statementRefs(): Refs[] {
    return this.#statementRefs;
  }

  private constructor(properties: LogicProperties) {
    this.#parsedLogic = properties.parsedLogic;
    this.#statementRefs = properties.statementRefs;
  }

  static #appendPath = (key: string, path: string): string => {
    let newPath = path;
    newPath += !path ? key : `.${key}`;
    return newPath;
  };

  // Checks if ref is a self ref and, hence, defines the target column and table itself
  static #isSelfRef = (path: string): boolean => {
    const selfElements = [
      `${SQLElement.CREATE_TABLE_STATEMENT}.${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.FROM_EXPRESSION_ELEMENT}.${SQLElement.TABLE_EXPRESSION}.${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.COMMON_TABLE_EXPRESSION}`,
    ];

    return selfElements.some((element) => path.includes(element));
  };

  static #getColumnDependencyType = (path: string): DependencyType => {
    const definitionElements = [
      `${SQLElement.COLUMN_DEFINITION}.${SQLElement.IDENTIFIER}`,
    ];

    const dataDependencyElements = [
      `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.COLUMN_REFERENCE}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.WILDCARD_EXPRESSION}.${SQLElement.WILDCARD_IDENTIFIER}`,
      `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.ALIAS_EXPRESSION}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.FUNCTION}.${SQLElement.BRACKETED}.${SQLElement.EXPRESSION}.${SQLElement.COLUMN_REFERENCE}.${SQLElement.IDENTIFIER}`,
    ];

    if (definitionElements.some((element) => path.includes(element)))
      return DependencyType.DEFINITION;
    if (dataDependencyElements.some((element) => path.includes(element)))
      return DependencyType.DATA;
    return DependencyType.QUERY;
  };

  static #joinArrayValue = (value: [{ [key: string]: string }]): string => {
    const valueElements = value.map((element) => {
      const identifierKey = 'identifier';
      const dotKey = 'dot';
      if (identifierKey in element) return element[identifierKey];
      if (dotKey in element) return element[dotKey];
      throw new RangeError('Unhandled value chaining error');
    });

    return valueElements.join('');
  };

  // Handles any column ref
  static #handleColumnIdentifierRef = (
    props: HandlerProperties<string>
  ): ColumnRef => {
    const columnValueRef = this.#splitColumnValue(props.value);
    const path = this.#appendPath(props.key, props.refPath);

    return {
      dependencyType: this.#getColumnDependencyType(path),
      path,
      alias: props.alias,
      name: columnValueRef.columnName,
      tableName: columnValueRef.tableName,
      schemaName: columnValueRef.schemaName,
      databaseName: columnValueRef.databaseName,
      warehouseName: columnValueRef.warehouseName,
      isWildcardRef: false,
    };
  };

  // Handles any table ref
  static #handleTableIdentifierRef = (
    props: HandlerProperties<string>
  ): TableRef => {
    const tableValueRef = this.#splitTableValue(props.value);
    const path = this.#appendPath(props.key, props.refPath);

    return {
      isSelfRef: this.#isSelfRef(path),
      path,
      alias: props.alias,
      name: tableValueRef.tableName,
      schemaName: tableValueRef.schemaName,
      databaseName: tableValueRef.databaseName,
      warehouseName: tableValueRef.warehouseName,
    };
  };

  // Handles any case of * (wildcard) refs
  static #handleWildCardRef = (
    props: HandlerProperties<{ [key: string]: any }>
  ): ColumnRef => {
    const wildcardElementKeys = Object.keys(props.value);

    const isDotNoation = [
      SQLElement.WILDCARD_IDENTIFIER_DOT,
      SQLElement.WILDCARD_IDENTIFIER_IDENTIFIER,
      SQLElement.WILDCARD_IDENTIFIER_STAR,
    ].every((wildcardElementKey) =>
      wildcardElementKeys.includes(wildcardElementKey)
    );

    const path = this.#appendPath(props.key, props.refPath);

    if (isDotNoation)
      return {
        dependencyType: this.#getColumnDependencyType(path),
        path,
        name: SQLElement.WILDCARD_IDENTIFIER_STAR,
        tableName: props.value[SQLElement.WILDCARD_IDENTIFIER_IDENTIFIER],
        isWildcardRef: true,
      };
    if (
      wildcardElementKeys.length === 1 &&
      wildcardElementKeys.includes(SQLElement.WILDCARD_IDENTIFIER_STAR)
    )
      return {
        dependencyType: this.#getColumnDependencyType(path),
        path,
        name: SQLElement.WILDCARD_IDENTIFIER_STAR,
        isWildcardRef: true,
      };
    throw new SyntaxError('Unhandled wildcard-dict use-case');
  };

  // todo - Will probably fail, e.g. when 'use schema' is used
  // Splits up warehouse.database.schema.table notation
  static #splitTableValue = (
    value: string
  ): {
    tableName: string;
    schemaName?: string;
    databaseName?: string;
    warehouseName?: string;
  } => {
    const valueElements = value.split('.').reverse();

    return {
      tableName: valueElements[0],
      schemaName: valueElements[1],
      databaseName: valueElements[2],
      warehouseName: valueElements[3],
    };
  };

  // todo - Will probably fail, e.g. when 'use schema' is used
  // Splits up warehouse.database.schema.table.column notation
  static #splitColumnValue = (
    value: string
  ): {
    columnName: string;
    tableName?: string;
    schemaName?: string;
    databaseName?: string;
    warehouseName?: string;
  } => {
    const valueElements = value.split('.').reverse();

    return {
      columnName: valueElements[0],
      tableName: valueElements[1],
      schemaName: valueElements[2],
      databaseName: valueElements[3],
      warehouseName: valueElements[4],
    };
  };

  // Runs through parse SQL logic (JSON object) and check for potential dependencies. Return those dependencies.
  static #extractRefs = (
    parsedSQL: { [key: string]: any },
    path = ''
  ): Refs => {
    let refPath = path;
    const refs: Refs = {
      columns: [],
      tables: [],
      wildcards: [],
    };

    let alias: string;
    Object.entries(parsedSQL).forEach((parsedSQLElement) => {
      const key = parsedSQLElement[0];
      const value = parsedSQLElement[1];

      refPath =
        key === SQLElement.KEYWORD && value === SQLElement.KEYWORD_AS
          ? this.#appendPath(value, refPath)
          : refPath;

      if (key === SQLElement.IDENTIFIER) {
        if (path.includes(SQLElement.ALIAS_EXPRESSION))
            alias = value;
        else if (path.includes(SQLElement.COLUMN_REFERENCE)){
          refs.columns.push(
            this.#handleColumnIdentifierRef({ key, value, refPath, alias })
          );

          alias = '';
        }
        else if (
          path.includes(SQLElement.TABLE_REFERENCE) ||
          path.includes(SQLElement.WITH_COMPOUND_STATEMENT)
        ) {
          const ref = this.#handleTableIdentifierRef({
            key,
            value,
            refPath,
            alias
          });

          alias = '';

          if (
            ref.isSelfRef &&
            refs.tables.filter((table) => table.isSelfRef).length
          )
            throw new RangeError(
              'Multiple self refs of model materialization found'
            );

          refs.tables.push(ref);
        }
      } else if (key === SQLElement.WILDCARD_IDENTIFIER)
        refs.wildcards.push(this.#handleWildCardRef({ key, value, refPath }));
      else if (value.constructor === Object) {
        const subRefs = this.#extractRefs(
          value,
          this.#appendPath(key, refPath)
        );

        refs.columns.push(...subRefs.columns);
        refs.tables.push(...subRefs.tables);
        refs.wildcards.push(...subRefs.wildcards);
      } else if (Object.prototype.toString.call(value) === '[object Array]') {
        if (key === SQLElement.COLUMN_REFERENCE)
          refs.columns.push(
            this.#handleColumnIdentifierRef({
              key,
              value: this.#joinArrayValue(value),
              refPath,
            })
          );
        else if (
          key === SQLElement.TABLE_REFERENCE ||
          key === SQLElement.COMMON_TABLE_EXPRESSION
        )
          refs.tables.push(
            this.#handleTableIdentifierRef({
              key,
              value: this.#joinArrayValue(value),
              refPath,
            })
          );
        else
          value.forEach((element: { [key: string]: any }) => {
            const subRefs = this.#extractRefs(
              element,
              this.#appendPath(key, refPath)
            );

            refs.columns.push(...subRefs.columns);
            refs.tables.push(...subRefs.tables);
            refs.wildcards.push(...subRefs.wildcards);
          });
      }
    });
    return refs;
  };

  static #hasMissingTableRefs = (statementRefs: Refs[]): boolean =>
    statementRefs.some(
      (element) =>
        element.columns.some((column) => !column.tableName) ||
        element.wildcards.some((wildcard) => !wildcard.tableName)
    );

  static #getBestMatchingTable = (
    columnPath: string,
    tables: TableRef[]
  ): TableRef => {
    const columnPathElements = columnPath.split('.');

    let bestMatch: { ref: TableRef; matchingPoints: number } | undefined;
    tables.forEach((table) => {
      const tablePathElements = table.path.split('.');

      let matchingPoints = 0;
      let differenceFound: boolean;
      columnPathElements.forEach((element, index) => {
        if (differenceFound) return;
        if (element === tablePathElements[index]) matchingPoints += 1;
        else differenceFound = true;
      });

      if (!bestMatch || matchingPoints > bestMatch.matchingPoints)
        bestMatch = { ref: table, matchingPoints };
      else if (bestMatch.matchingPoints === matchingPoints)
        throw new RangeError(
          'More than one potential table match found for column reference'
        );
    });

    if (!bestMatch)
      throw new ReferenceError('No table match for column reference found');

    return bestMatch.ref;
  };

  static #addColumnTableInfo = (statementRefs: Refs[]): Refs[] => {
    const fixedStatementRefs: Refs[] = statementRefs.map((element) => {
      const parentTables = element.tables.filter((table) => !table.isSelfRef);

      const columns = element.columns.map((column) => {
        if (column.tableName) return column;

        const columnToFix = column;

        const tableRef = this.#getBestMatchingTable(
          columnToFix.path,
          parentTables
        );

        columnToFix.tableName = tableRef.name;
        columnToFix.schemaName = tableRef.schemaName;
        columnToFix.databaseName = tableRef.databaseName;
        columnToFix.warehouseName = tableRef.warehouseName;

        return columnToFix;
      });

      const wildcards = element.wildcards.map((wildcard) => {
        if (wildcard.tableName) return wildcard;

        const wildcardToFix = wildcard;

        const tableRef = this.#getBestMatchingTable(
          wildcardToFix.path,
          parentTables
        );

        wildcardToFix.tableName = tableRef.name;
        wildcardToFix.schemaName = tableRef.schemaName;
        wildcardToFix.databaseName = tableRef.databaseName;
        wildcardToFix.warehouseName = tableRef.warehouseName;

        return wildcardToFix;
      });

      return { tables: element.tables, columns, wildcards };
    });

    return fixedStatementRefs;
  };

  // Runs through tree of parsed logic and extract all refs of tables and columns (self and parent tables)
  static #getStatementRefs = (fileObj: any): Refs[] => {
    let statementRefs: Refs[] = [];

    if (
      fileObj.constructor === Object &&
      fileObj[SQLElement.STATEMENT] !== undefined
    ) {
      const statementRefsObj = this.#extractRefs(fileObj[SQLElement.STATEMENT]);
      statementRefs.push(statementRefsObj);
    } else if (Object.prototype.toString.call(fileObj) === '[object Array]') {
      const statementObjects = fileObj.filter(
        (statement: any) => SQLElement.STATEMENT in statement
      );

      statementObjects.forEach((statement: any) => {
        const statementRefsObj = this.#extractRefs(
          statement[SQLElement.STATEMENT]
        );
        statementRefs.push(statementRefsObj);
      });
    }

    if (this.#hasMissingTableRefs(statementRefs))
      statementRefs = this.#addColumnTableInfo(statementRefs);

    if (this.#hasMissingTableRefs(statementRefs))
      throw new ReferenceError('Missing table for column reference');

    return statementRefs;
  };

  static create(prototype: LogicPrototype): Logic {
    if (!prototype.parsedLogic)
      throw new TypeError('Logic object must have parsed logic');

    const parsedLogicObj = JSON.parse(prototype.parsedLogic);

    const statementRefs = this.#getStatementRefs(parsedLogicObj.file);

    const logic = new Logic({
      parsedLogic: prototype.parsedLogic,
      statementRefs,
    });

    return logic;
  }
}