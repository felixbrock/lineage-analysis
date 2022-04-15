import SQLElement from './sql-element';

enum RefType {
  TABLE = 'TABLE',
  COLUMN = 'COLUMN',
}

interface Ref {
  path: string;
  name: string;
  isSelfRef: boolean;
  schemaName?: string;
  databaseName?: string;
  warehouseName?: string;
}

export type TableRef = Ref;

export interface ColumnRef extends Ref {
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
  static #isSelfRef = (path: string, refType: RefType): boolean => {
    const columnSelfRefs =
      refType === RefType.TABLE
        ? [
            `${SQLElement.CREATE_TABLE_STATEMENT}.${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`,
            `${SQLElement.FROM_EXPRESSION_ELEMENT}.${SQLElement.TABLE_EXPRESSION}.${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`,
          ]
        : [
            `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.COLUMN_REFERENCE}.${SQLElement.IDENTIFIER}`,
            `${SQLElement.COLUMN_DEFINITION}.${SQLElement.IDENTIFIER}`,
            `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.WILDCARD_EXPRESSION}.${SQLElement.WILDCARD_IDENTIFIER}`,
          ];

    return columnSelfRefs.some((ref) => path.includes(ref));
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
      isSelfRef: this.#isSelfRef(path, RefType.COLUMN),
      path,
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
      isSelfRef: this.#isSelfRef(path, RefType.TABLE),
      path,
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
        isSelfRef: true,
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
        isSelfRef: true,
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

    Object.entries(parsedSQL).forEach((parsedSQLElement) => {
      const key = parsedSQLElement[0];
      const value = parsedSQLElement[1];

      refPath =
        key === SQLElement.KEYWORD && value === SQLElement.KEYWORD_AS
          ? this.#appendPath(value, refPath)
          : refPath;

      if (key === SQLElement.IDENTIFIER) {
        if (path.includes(SQLElement.COLUMN_REFERENCE))
          refs.columns.push(
            this.#handleColumnIdentifierRef({ key, value, refPath })
          );
        else if (path.includes(SQLElement.TABLE_REFERENCE)) {
          const ref = this.#handleTableIdentifierRef({
            key,
            value,
            refPath,
          });

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
        else if (key === SQLElement.TABLE_REFERENCE)
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

  // Runs through tree of parsed logic and extract all refs of tables and columns (self and parent tables)
  static #getStatementRefs = (fileObj: any): Refs[] => {
    const statementRefs: Refs[] = [];

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
