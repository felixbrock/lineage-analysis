import SQLElement from './sql-element';

export enum ReferenceType {
  TABLE = 'TABLE',
  COLUMN = 'COLUMN',
  WILDCARD = 'WILDCARD',
}

export interface StatementReference {
  path: string;
  type: ReferenceType;
  columnName?: string;
  tableName?: string;
}

interface LogicProperties {
  parsedLogic: string;
  statementReferences: StatementReference[][];
}

export interface LogicPrototype {
  parsedLogic: string;
}

interface HandlerProperties<T> {
  key: string;
  value: T;
  referencePath: string;
}

export class Logic {
  #parsedLogic: string;

  #statementReferences: StatementReference[][];

  get parsedLogic(): string {
    return this.#parsedLogic;
  }

  get statementReferences(): StatementReference[][] {
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

  static #handleIdentifierRef = (
    props: HandlerProperties<string>
  ): StatementReference => {
    const type = props.referencePath.includes(SQLElement.TABLE_REFERENCE)
      ? ReferenceType.TABLE
      : ReferenceType.COLUMN;

    const columnValueReference =
      type === ReferenceType.COLUMN
        ? this.#getColumnValueReference(props.value)
        : undefined;

    return {
      path: this.#appendPath(props.key, props.referencePath),
      type,
      columnName: columnValueReference
        ? columnValueReference.columnName
        : props.value,
      tableName: columnValueReference
        ? columnValueReference.tableName
        : props.value,
    };
  };

  static #handleWildCardRef = (
    props: HandlerProperties<{ [key: string]: any }>
  ): StatementReference => {
    const wildcardElementKeys = Object.keys(props.value);

    const isDotNoation = [
      SQLElement.WILDCARD_IDENTIFIER_DOT,
      SQLElement.WILDCARD_IDENTIFIER_IDENTIFIER,
      SQLElement.WILDCARD_IDENTIFIER_STAR,
    ].every((wildcardElementKey) =>
      wildcardElementKeys.includes(wildcardElementKey)
    );

    if (isDotNoation)
      return {
        path: this.#appendPath(props.key, props.referencePath),
        name: `${value[SQLElement.WILDCARD_IDENTIFIER_IDENTIFIER]}${
          value[SQLElement.WILDCARD_IDENTIFIER_DOT]
        }${value[SQLElement.WILDCARD_IDENTIFIER_STAR]}`,
      };
    if (
      wildcardElementKeys.length === 1 &&
      wildcardElementKeys.includes(SQLElement.WILDCARD_IDENTIFIER_STAR)
    )
      return {
        path: this.#appendPath(props.key, props.referencePath),
        name: value[SQLElement.WILDCARD_IDENTIFIER_STAR],
      };
    throw new SyntaxError('Unhandled wildcard-dict use-case');
  };

  static #handleColumnRef = (
    props: HandlerProperties<{ [key: string]: any }[]>
  ): StatementReference => {
    let valuePath = '';
    let keyPath = '';
    props.value.forEach((valueElement) => {
      const subStatementReferences = this.#extractstatementReferences(
        valueElement,
        this.#appendPath(props.key, props.referencePath)
      );
      subStatementReferences.forEach((dependencyElement) => {
        valuePath = this.#appendPath(dependencyElement.name, valuePath);
        keyPath = dependencyElement.path;
      });
    });
    return { path: keyPath, name: valuePath };
  };

  static #getColumnValueReference = (
    columnValue: string
  ): { columnName?: string; tableName?: string } => {
    const columnName = columnValue.includes('.')
      ? columnValue.split('.').slice(-1)[0]
      : columnValue;
    const tableName = columnValue.includes('.')
      ? columnValue.split('.').slice(0)[0]
      : '';

    return { columnName, tableName };
  };

  static #extractstatementReferences = (
    parsedSQL: { [key: string]: any },
    path = ''
  ): StatementReference[] => {
    let referencePath = path;
    const statementReferences: StatementReference[] = [];

    Object.entries(parsedSQL).forEach((parsedSQLElement) => {
      const key = parsedSQLElement[0];
      const value = parsedSQLElement[1];

      referencePath =
        key === SQLElement.KEYWORD && value === SQLElement.KEYWORD_AS
          ? this.#appendPath(value, referencePath)
          : referencePath;

      if (key === SQLElement.IDENTIFIER)
        statementReferences.push(
          this.#handleIdentifierRef({ key, value, referencePath })
        );
      else if (key === SQLElement.WILDCARD_IDENTIFIER)
        statementReferences.push(
          this.#handleWildCardRef({ key, value, referencePath })
        );
      else if (key === SQLElement.COLUMN_REFERENCE) {
        if (Object.prototype.toString.call(value) === '[object Array]')
          statementReferences.push(
            this.#handleColumnRef({ key, value, referencePath })
          );
      } else if (value.constructor === Object) {
        const subStatementReferences = this.#extractstatementReferences(
          value,
          this.#appendPath(key, referencePath)
        );
        subStatementReferences.forEach((dependencyElement) =>
          statementReferences.push(dependencyElement)
        );
      } else if (Object.prototype.toString.call(value) === '[object Array]') {
        value.forEach((valueElement: { [key: string]: any }) => {
          const subStatementReferences = this.#extractstatementReferences(
            valueElement,
            this.#appendPath(key, referencePath)
          );
          subStatementReferences.forEach((dependencyElement) =>
            statementReferences.push(dependencyElement)
          );
        });
      }
    });
    return statementReferences;
  };

  static #getstatementReferences = (fileObj: any): StatementReference[][] => {
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

  static create(prototype: LogicPrototype): Logic {
    if (!prototype.parsedLogic)
      throw new TypeError('Logic object must have parsed logic');

    const parsedLogicObj = JSON.parse(prototype.parsedLogic);

    const statementReferences = this.#getstatementReferences(
      parsedLogicObj.file
    );

    const logic = new Logic({
      parsedLogic: prototype.parsedLogic,
      statementReferences,
    });

    return logic;
  }
}
