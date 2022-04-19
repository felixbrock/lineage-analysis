import { DependencyType } from './dependency';
import SQLElement from '../value-types/sql-element';

export interface LogicProperties {
  id: string;
  dbtModelId: string;
  parsedLogic: string;
  statementRefs: Refs[];
  lineageId: string;
}

export interface LogicPrototype {
  id: string;
  dbtModelId: string;
  parsedLogic: string;
  lineageId: string;
}

interface Ref {
  path: string;
  name: string;
  alias?: string;
  schemaName?: string;
  databaseName?: string;
  warehouseName?: string;
}

export interface MaterializationRef extends Ref {
  isSelfRef: boolean;
}

export interface ColumnRef extends Ref {
  dependencyType: DependencyType;
  isWildcardRef: boolean;
  materializationName?: string;
}

export interface Refs {
  materializations: MaterializationRef[];
  columns: ColumnRef[];
  wildcards: ColumnRef[];
}


interface HandlerProperties<ValueType> {
  key: string;
  value: ValueType;
  alias?: string;
  refPath: string;
}

export class Logic {

  #id: string;

  #dbtModelId: string;

  #parsedLogic: string;

  #statementRefs: Refs[];

  #lineageId: string;

  get id(): string {
    return this.#id;
  }

  get dbtModelId(): string {
    return this.#dbtModelId;
  }

  get parsedLogic(): string {
    return this.#parsedLogic;
  }

  get statementRefs(): Refs[] {
    return this.#statementRefs;
  }

  get lineageId(): string {
    return this.#lineageId;
  }

  private constructor(properties: LogicProperties) {
    this.#id = properties.id;
    this.#dbtModelId = properties.dbtModelId;
    this.#parsedLogic = properties.parsedLogic;
    this.#statementRefs = properties.statementRefs;
    this.#lineageId = properties.lineageId;
  }

  static #appendPath = (key: string, path: string): string => {
    let newPath = path;
    newPath += !path ? key : `.${key}`;
    return newPath;
  };

  // Checks if ref is a self ref and, hence, defines the target column and materialization itself
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
      materializationName: columnValueRef.materializationName,
      schemaName: columnValueRef.schemaName,
      databaseName: columnValueRef.databaseName,
      warehouseName: columnValueRef.warehouseName,
      isWildcardRef: false,
    };
  };

  // Handles any materialization ref
  static #handleMaterializationIdentifierRef = (
    props: HandlerProperties<string>
  ): MaterializationRef => {
    const materializationValueRef = this.#splitMaterializationValue(props.value);
    const path = this.#appendPath(props.key, props.refPath);

    return {
      isSelfRef: this.#isSelfRef(path),
      path,
      alias: props.alias,
      name: materializationValueRef.materializationName,
      schemaName: materializationValueRef.schemaName,
      databaseName: materializationValueRef.databaseName,
      warehouseName: materializationValueRef.warehouseName,
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
        materializationName: props.value[SQLElement.WILDCARD_IDENTIFIER_IDENTIFIER],
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
  // Splits up warehouse.database.schema.materialization notation
  static #splitMaterializationValue = (
    value: string
  ): {
    materializationName: string;
    schemaName?: string;
    databaseName?: string;
    warehouseName?: string;
  } => {
    const valueElements = value.split('.').reverse();

    return {
      materializationName: valueElements[0],
      schemaName: valueElements[1],
      databaseName: valueElements[2],
      warehouseName: valueElements[3],
    };
  };

  // todo - Will probably fail, e.g. when 'use schema' is used
  // Splits up warehouse.database.schema.materialization.column notation
  static #splitColumnValue = (
    value: string
  ): {
    columnName: string;
    materializationName?: string;
    schemaName?: string;
    databaseName?: string;
    warehouseName?: string;
  } => {
    const valueElements = value.split('.').reverse();

    return {
      columnName: valueElements[0],
      materializationName: valueElements[1],
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
      materializations: [],
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
          const ref = this.#handleMaterializationIdentifierRef({
            key,
            value,
            refPath,
            alias
          });

          alias = '';

          if (
            ref.isSelfRef &&
            refs.materializations.filter((materialization) => materialization.isSelfRef).length
          )
            throw new RangeError(
              'Multiple self refs of logic materialization found'
            );

          refs.materializations.push(ref);
        }
      } else if (key === SQLElement.WILDCARD_IDENTIFIER)
        refs.wildcards.push(this.#handleWildCardRef({ key, value, refPath }));
      else if (value.constructor === Object) {
        const subRefs = this.#extractRefs(
          value,
          this.#appendPath(key, refPath)
        );

        refs.columns.push(...subRefs.columns);
        refs.materializations.push(...subRefs.materializations);
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
          refs.materializations.push(
            this.#handleMaterializationIdentifierRef({
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
            refs.materializations.push(...subRefs.materializations);
            refs.wildcards.push(...subRefs.wildcards);
          });
      }
    });
    return refs;
  };

  static #hasMissingMaterializationRefs = (statementRefs: Refs[]): boolean =>
    statementRefs.some(
      (element) =>
        element.columns.some((column) => !column.materializationName) ||
        element.wildcards.some((wildcard) => !wildcard.materializationName)
    );

  static #getBestMatchingMaterialization = (
    columnPath: string,
    materializations: MaterializationRef[]
  ): MaterializationRef => {
    const columnPathElements = columnPath.split('.');

    let bestMatch: { ref: MaterializationRef; matchingPoints: number } | undefined;
    materializations.forEach((materialization) => {
      const materializationPathElements = materialization.path.split('.');

      let matchingPoints = 0;
      let differenceFound: boolean;
      columnPathElements.forEach((element, index) => {
        if (differenceFound) return;
        if (element === materializationPathElements[index]) matchingPoints += 1;
        else differenceFound = true;
      });

      if (!bestMatch || matchingPoints > bestMatch.matchingPoints)
        bestMatch = { ref: materialization, matchingPoints };
      else if (bestMatch.matchingPoints === matchingPoints)
        throw new RangeError(
          'More than one potential materialization match found for column reference'
        );
    });

    if (!bestMatch)
      throw new ReferenceError('No materialization match for column reference found');

    return bestMatch.ref;
  };

  static #addColumnMaterializationInfo = (statementRefs: Refs[]): Refs[] => {
    const fixedStatementRefs: Refs[] = statementRefs.map((element) => {
      const parentMaterializations = element.materializations.filter((materialization) => !materialization.isSelfRef);

      const columns = element.columns.map((column) => {
        if (column.materializationName) return column;

        const columnToFix = column;

        const materializationRef = this.#getBestMatchingMaterialization(
          columnToFix.path,
          parentMaterializations
        );

        columnToFix.materializationName = materializationRef.name;
        columnToFix.schemaName = materializationRef.schemaName;
        columnToFix.databaseName = materializationRef.databaseName;
        columnToFix.warehouseName = materializationRef.warehouseName;

        return columnToFix;
      });

      const wildcards = element.wildcards.map((wildcard) => {
        if (wildcard.materializationName) return wildcard;

        const wildcardToFix = wildcard;

        const materializationRef = this.#getBestMatchingMaterialization(
          wildcardToFix.path,
          parentMaterializations
        );

        wildcardToFix.materializationName = materializationRef.name;
        wildcardToFix.schemaName = materializationRef.schemaName;
        wildcardToFix.databaseName = materializationRef.databaseName;
        wildcardToFix.warehouseName = materializationRef.warehouseName;

        return wildcardToFix;
      });

      return { materializations: element.materializations, columns, wildcards };
    });

    return fixedStatementRefs;
  };

  // Runs through tree of parsed logic and extract all refs of materializations and columns (self and parent materializations)
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

    if (this.#hasMissingMaterializationRefs(statementRefs))
      statementRefs = this.#addColumnMaterializationInfo(statementRefs);

    if (this.#hasMissingMaterializationRefs(statementRefs))
      throw new ReferenceError('Missing materialization for column reference');

    return statementRefs;
  };

  static create(prototype: LogicPrototype): Logic {
    if (!prototype.id) throw new TypeError('Logic must have id');
    if (!prototype.dbtModelId) throw new TypeError('Logic must have dbtModelId');
    if (!prototype.parsedLogic)
      throw new TypeError('Logic creation requires parsed SQL logic');
    if (!prototype.lineageId) throw new TypeError('Logic must have lineageId');

    const parsedLogicObj = JSON.parse(prototype.parsedLogic);

    const statementRefs = this.#getStatementRefs(parsedLogicObj.file);

    const logic = new Logic({
      id: prototype.id,
      dbtModelId: prototype.dbtModelId,
      parsedLogic: prototype.parsedLogic,
      statementRefs,
      lineageId: prototype.lineageId,
    });

    return logic;
  }
}