import SQLElement from './sql-element';

enum RefType {
  TABLE = 'TABLE',
  COLUMN = 'COLUMN',
}

interface Ref {
  path: string;
  name: string;
  isSelfRef: boolean;
}

type TableRef = Ref;

interface ColumnRef extends Ref {
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
  statementReferences: Refs[];
}

export interface LogicPrototype {
  parsedLogic: string;
}

interface HandlerProperties<ValueType> {
  key: string;
  value: ValueType;
  referencePath: string;
}

export class Logic {
  #parsedLogic: string;

  #statementReferences: Refs[];

  get parsedLogic(): string {
    return this.#parsedLogic;
  }

  get statementReferences(): Refs[] {
    return this.#statementReferences;
  }

  private constructor(properties: LogicProperties) {
    this.#parsedLogic = properties.parsedLogic;
    this.#statementReferences = properties.statementReferences;
  }

  static #appendPath = (key: string, path: string): string => {
    let newPath = path;
    newPath += !path ? key : `.${key}`;
    return newPath;
  };

  // Checks if reference is a self reference and, hence, defines the target column and table itself
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

  // Handles any column reference
  static #handleColumnIdentifierRef = (
    props: HandlerProperties<string>
  ): ColumnRef => {
    const columnValueRef = this.#getColumnValueReference(props.value);
    const path = this.#appendPath(props.key, props.referencePath);

    return {
      isSelfRef: this.#isSelfRef(path, RefType.COLUMN),
      path,
      name: columnValueRef.columnName,
      tableName: columnValueRef.tableName,
      isWildcardRef: false,
    };
  };

  // Handles any table reference
  static #handleTableIdentifierRef = (
    props: HandlerProperties<string>
  ): TableRef => {
    const path = this.#appendPath(props.key, props.referencePath);

    return {
      isSelfRef: this.#isSelfRef(path, RefType.TABLE),
      path,
      name: props.value,
    };
  };

  // Handles any case of * (wildcard) references
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

    const path = this.#appendPath(props.key, props.referencePath);

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

  // Splits up table.column notation
  static #getColumnValueReference = (
    columnValue: string
  ): { columnName: string; tableName?: string } => {
    const columnName = columnValue.includes('.')
      ? columnValue.split('.').slice(-1)[0]
      : columnValue;
    const tableName = columnValue.includes('.')
      ? columnValue.split('.').slice(0)[0]
      : undefined;

    return { columnName, tableName };
  };

  // Runs through parse SQL logic (JSON object) and check for potential dependencies. Return those dependencies.
  static #extractReferences = (
    parsedSQL: { [key: string]: any },
    path = ''
  ): Refs => {
    let referencePath = path;
    const refs: Refs = {
      columns: [],
      tables: [],
      wildcards: [],
    };

    Object.entries(parsedSQL).forEach((parsedSQLElement) => {
      const key = parsedSQLElement[0];
      const value = parsedSQLElement[1];

      referencePath =
        key === SQLElement.KEYWORD && value === SQLElement.KEYWORD_AS
          ? this.#appendPath(value, referencePath)
          : referencePath;

      if (key === SQLElement.IDENTIFIER)
        if (path.includes(SQLElement.COLUMN_REFERENCE))
          refs.columns.push(
            this.#handleColumnIdentifierRef({ key, value, referencePath })
          );
        else if (path.includes(SQLElement.TABLE_REFERENCE)) {
          const ref = this.#handleTableIdentifierRef({
            key,
            value,
            referencePath,
          });

          if (
            ref.isSelfRef &&
            refs.tables.filter((table) => table.isSelfRef).length
          )
            throw new RangeError(
              'Multiple self references of model materialization found'
            );

          refs.tables.push(ref);
        } else if (key === SQLElement.WILDCARD_IDENTIFIER)
          refs.wildcards.push(
            this.#handleWildCardRef({ key, value, referencePath })
          );
        else if (value.constructor === Object) {
          const subRefs = this.#extractReferences(
            value,
            this.#appendPath(key, referencePath)
          );

          refs.columns.push(...subRefs.columns);
          refs.tables.push(...subRefs.tables);
          refs.wildcards.push(...subRefs.wildcards);
        } else if (Object.prototype.toString.call(value) === '[object Array]') {
          if (key === SQLElement.COLUMN_REFERENCE)
            refs.columns.push(
              this.#handleColumnIdentifierRef({
                key,
                value: value.join(''),
                referencePath,
              })
            );
          else
            value.forEach((element: { [key: string]: any }) => {
              const subRefs = this.#extractReferences(
                element,
                this.#appendPath(key, referencePath)
              );

              refs.columns.push(...subRefs.columns);
              refs.tables.push(...subRefs.tables);
              refs.wildcards.push(...subRefs.wildcards);
            });
        }
    });
    return refs;
  };

  // Runs through tree of parsed logic and extract all references of tables and columns (self and parent tables)
  static #getStatementReferences = (fileObj: any): Refs[] => {
    const statementReferences: Refs[] = [];

    if (
      fileObj.constructor === Object &&
      fileObj[SQLElement.STATEMENT] !== undefined
    ) {
      const statementReferencesObj = this.#extractReferences(
        fileObj[SQLElement.STATEMENT]
      );
      statementReferences.push(statementReferencesObj);
    } else if (Object.prototype.toString.call(fileObj) === '[object Array]') {
      const statementObjects = fileObj.filter(
        (statement: any) => SQLElement.STATEMENT in statement
      );

      statementObjects.forEach((statement: any) => {
        const statementReferencesObj = this.#extractReferences(
          statement[SQLElement.STATEMENT]
        );
        statementReferences.push(statementReferencesObj);
      });
    }

    return statementReferences;
  };

  static create(prototype: LogicPrototype): Logic {
    if (!prototype.parsedLogic)
      throw new TypeError('Logic object must have parsed logic');

    const parsedLogicObj = JSON.parse(prototype.parsedLogic);

    const statementReferences = this.#getStatementReferences(
      parsedLogicObj.file
    );

    const logic = new Logic({
      parsedLogic: prototype.parsedLogic,
      statementReferences,
    });

    return logic;
  }
}
