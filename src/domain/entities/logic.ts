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
  catalog: CatalogModelData[];
}

export interface CatalogModelData {
  modelName: string;
  materialisationName: string;
  columnNames: string[];
}
interface Ref {
  name: string;
  alias?: string;
  schemaName?: string;
  databaseName?: string;
  warehouseName?: string;
}

export interface MaterializationRef extends Ref {
  paths: string[];
  isSelfRef: boolean;
}

interface ColumnRefPrototype extends Ref {
  path: string;
  dependencyType: DependencyType;
  isWildcardRef: boolean;
  materializationName?: string;
}

export interface ColumnRef
  extends Omit<ColumnRefPrototype, 'materializationName'> {
  materializationName: string;
}

interface RefsPrototype {
  materializations: MaterializationRef[];
  columns: ColumnRefPrototype[];
  wildcards: ColumnRefPrototype[];
}

export interface Refs extends Omit<RefsPrototype, 'columns' | 'wildcards'> {
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

  /* Adds a key to an already existing path that describes the current exploration route through a json tree */
  static #appendPath = (key: string, path: string): string => {
    let newPath = path;
    newPath += !path ? key : `.${key}`;
    return newPath;
  };

  /* Checks if a table ref is describing the resulting materialization of an corresponding SQL model */
  static #isSelfRef = (path: string): boolean => {
    const selfElements = [
      `${SQLElement.CREATE_TABLE_STATEMENT}.${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.FROM_EXPRESSION_ELEMENT}.${SQLElement.TABLE_EXPRESSION}.${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.COMMON_TABLE_EXPRESSION}`,
    ];

    return selfElements.some((element) => path.includes(element));
  };

  /* Returns the type of dependency identified in the SQL logic */
  static #getColumnDependencyType = (path: string): DependencyType => {
    const definitionElements = [
      `${SQLElement.COLUMN_DEFINITION}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.ALIAS_EXPRESSION}.${SQLElement.IDENTIFIER}`,
    ];

    const dataDependencyElements = [
      `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.COLUMN_REFERENCE}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.WILDCARD_EXPRESSION}.${SQLElement.WILDCARD_IDENTIFIER}`,
      `${SQLElement.FUNCTION}.${SQLElement.BRACKETED}.${SQLElement.EXPRESSION}.${SQLElement.COLUMN_REFERENCE}.${SQLElement.IDENTIFIER}`,
    ];

    if (definitionElements.some((element) => path.includes(element)))
      return DependencyType.DEFINITION;
    if (dataDependencyElements.some((element) => path.includes(element)))
      return DependencyType.DATA;
    return DependencyType.QUERY;
  };

  /* Returns a single joined string value of an array value identified in the parsed SQL logic
  (e.g. for for table_expressions or column_expressions) */
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

  /* Handles any column ref found in the parsed SQL logic */
  static #handleColumnIdentifierRef = (
    props: HandlerProperties<string>
  ): ColumnRefPrototype => {
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

  /* Handles any materialization ref found in the parsed SQL logic */
  static #handleMaterializationIdentifierRef = (
    props: HandlerProperties<string>
  ): MaterializationRef => {
    const materializationValueRef = this.#splitMaterializationValue(
      props.value
    );
    const path = this.#appendPath(props.key, props.refPath);

    return {
      isSelfRef: this.#isSelfRef(path),
      paths: [path],
      alias: props.alias,
      name: materializationValueRef.materializationName,
      schemaName: materializationValueRef.schemaName,
      databaseName: materializationValueRef.databaseName,
      warehouseName: materializationValueRef.warehouseName,
    };
  };

  /* Handles any * (wildcard) ref found in the parsed SQL logic */
  static #handleWildCardRef = (
    props: HandlerProperties<{ [key: string]: any }>
  ): ColumnRefPrototype => {
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
        materializationName:
          props.value[SQLElement.WILDCARD_IDENTIFIER_IDENTIFIER],
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

  /* Splits up warehouse.database.schema.materialization notation */
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

  /* Splits up warehouse.database.schema.materialization.column notation */
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

  /* Adds a new materialization reference found safely to the existing array of already identified refs */
  static #pushMaterialization = (
    ref: MaterializationRef,
    refs: MaterializationRef[]
  ): MaterializationRef[] => {
    // todo - ignores the case .schema1.tableName and .schema2.tableName where both tables have the same name. Valid case, but logic will throw an error
    const findExistingInstance = (element: MaterializationRef): boolean => {
      if (ref.isSelfRef) return element.isSelfRef && element.name === ref.name;
      return !element.isSelfRef && element.name === ref.name;
    };

    const existingInstance = refs.find(findExistingInstance);

    if (!existingInstance) {
      refs.push(ref);
      return refs;
    }

    existingInstance.alias = existingInstance.alias || ref.alias;
    existingInstance.schemaName = existingInstance.schemaName || ref.schemaName;
    existingInstance.databaseName =
      existingInstance.databaseName || ref.databaseName;
    existingInstance.warehouseName =
      existingInstance.warehouseName || ref.warehouseName;

    existingInstance.paths.push(...ref.paths);

    return refs.map((element) =>
      findExistingInstance(element) ? existingInstance : element
    );
  };

  /* Directly interacts with parsed SQL logic. Calls different handlers based on use-case. 
  Runs through parse SQL logic (JSON object) and check for potential dependencies. */
  static #extractRefs = (
    parsedSQL: { [key: string]: any },
    path = ''
  ): RefsPrototype => {
    let refPath = path;

    const refsPrototype: RefsPrototype = {
      columns: [],
      materializations: [],
      wildcards: [],
    };

    let alias: HandlerProperties<string> = { key: '', value: '', refPath: '' };
    Object.entries(parsedSQL).forEach((parsedSQLElement) => {
      const key = parsedSQLElement[0];
      const value =
        typeof parsedSQLElement[1] === 'string'
          ? parsedSQLElement[1].toUpperCase()
          : parsedSQLElement[1];

      refPath =
        key === SQLElement.KEYWORD && value === SQLElement.KEYWORD_AS
          ? this.#appendPath(value, refPath)
          : refPath;

      if (key === SQLElement.IDENTIFIER) {
        if (path.includes(SQLElement.ALIAS_EXPRESSION))
          alias = { key, value, refPath };
        else if (path.includes(SQLElement.COLUMN_REFERENCE)) {
          refsPrototype.columns.push(
            this.#handleColumnIdentifierRef({
              key,
              value,
              refPath,
              alias: alias.value,
            })
          );

          alias = { key: '', value: '', refPath: '' };
        } else if (
          path.includes(SQLElement.TABLE_REFERENCE) ||
          (path.includes(`${SQLElement.COMMON_TABLE_EXPRESSION}`) &&
            !path.includes(`${SQLElement.COMMON_TABLE_EXPRESSION}.`))
        ) {
          const ref = this.#handleMaterializationIdentifierRef({
            key,
            value,
            refPath,
            alias: alias.value,
          });

          alias = { key: '', value: '', refPath: '' };

          refsPrototype.materializations = this.#pushMaterialization(
            ref,
            refsPrototype.materializations
          );
        }
      } else if (key === SQLElement.WILDCARD_IDENTIFIER)
        refsPrototype.wildcards.push(
          this.#handleWildCardRef({ key, value, refPath })
        );
      else if (value.constructor === Object) {
        const subRefs = this.#extractRefs(
          value,
          this.#appendPath(key, refPath)
        );

        refsPrototype.columns.push(...subRefs.columns);
        subRefs.materializations.forEach((materialization) => {
          refsPrototype.materializations = this.#pushMaterialization(
            materialization,
            refsPrototype.materializations
          );
        });
        refsPrototype.wildcards.push(...subRefs.wildcards);
      } else if (Object.prototype.toString.call(value) === '[object Array]') {
        if (key === SQLElement.COLUMN_REFERENCE)
          refsPrototype.columns.push(
            this.#handleColumnIdentifierRef({
              key,
              value: this.#joinArrayValue(value),
              refPath,
            })
          );
        else if (key === SQLElement.TABLE_REFERENCE) {
          const ref = this.#handleMaterializationIdentifierRef({
            key,
            value: this.#joinArrayValue(value),
            refPath,
          });

          refsPrototype.materializations = this.#pushMaterialization(
            ref,
            refsPrototype.materializations
          );
        } else
          value.forEach((element: { [key: string]: any }) => {
            const subRefs = this.#extractRefs(
              element,
              this.#appendPath(key, refPath)
            );

            refsPrototype.columns.push(...subRefs.columns);
            subRefs.materializations.forEach((materialization) => {
              refsPrototype.materializations = this.#pushMaterialization(
                materialization,
                refsPrototype.materializations
              );
            });
            refsPrototype.wildcards.push(...subRefs.wildcards);
          });
      }
    });

    // todo - Based on the assumption that unmatched alias only exist for columns and not tables
    if (alias.value)
      refsPrototype.columns.push(this.#handleColumnIdentifierRef(alias));

    alias = { key: '', value: '', refPath: '' };

    return refsPrototype;
  };

  /* Identifies the closest materialization reference to a provided column path. 
  Assumption: The closest materialization ref in SQL defines the the ref's materialization */
  static #getBestMatchingMaterialization = (
    columnPath: string,
    materializations: MaterializationRef[],
    columnName: string,
    catalog: CatalogModelData[]
  ): MaterializationRef => {
    const columnPathElements = columnPath.split('.');

    let bestMatch:
      | { ref: MaterializationRef; matchingPoints: number }
      | undefined;
    materializations.forEach((materialization) => {
      materialization.paths.forEach((path) => {
        const materializationPathElements = path.split('.');

        let matchingPoints = 0;
        let differenceFound: boolean;
        columnPathElements.forEach((element, index) => {
          if (differenceFound) return;
          if (element === materializationPathElements[index])
            matchingPoints += 1;
          else differenceFound = true;
        });

        if (!bestMatch || matchingPoints > bestMatch.matchingPoints)
          bestMatch = { ref: materialization, matchingPoints };
        else if (bestMatch.matchingPoints === matchingPoints) {
          const materializationName = materialization.name;

          catalog.forEach((modelData) => {
            if (
              modelData.materialisationName === materializationName &&
              modelData.columnNames.includes(columnName)
            )
              bestMatch = { ref: materialization, matchingPoints };
          });
        }
      });
    });

    if (!bestMatch)
      throw new ReferenceError(
        'No materialization match for column reference found'
      );

    return bestMatch.ref;
  };

  /* Transforms RefsPrototype object to Refs object by identifying missing materialization refs */
  static #buildStatementRefs = (
    statementRefs: RefsPrototype[],
    catalog: CatalogModelData[]
  ): Refs[] => {
    const fixedStatementRefs: Refs[] = statementRefs.map((element) => {
      const columns: ColumnRef[] = element.columns.map((column) => {
        if (column.materializationName)
          return {
            dependencyType: column.dependencyType,
            name: column.name,
            alias: column.alias,
            path: column.path,
            isWildcardRef: column.isWildcardRef,
            materializationName: column.materializationName,
            schemaName: column.schemaName,
            databaseName: column.databaseName,
            warehouseName: column.warehouseName,
          };

        const columnToFix = column;

        const materializationRef = this.#getBestMatchingMaterialization(
          columnToFix.path,
          element.materializations,
          column.name,
          catalog
        );

        return {
          dependencyType: columnToFix.dependencyType,
          name: columnToFix.name,
          alias: columnToFix.alias,
          path: columnToFix.path,
          isWildcardRef: columnToFix.isWildcardRef,
          materializationName: materializationRef.name,
          schemaName: materializationRef.schemaName,
          databaseName: materializationRef.databaseName,
          warehouseName: materializationRef.warehouseName,
        };
      });

      const wildcards: ColumnRef[] = element.wildcards.map((wildcard) => {
        if (wildcard.materializationName)
          return {
            dependencyType: wildcard.dependencyType,
            name: wildcard.name,
            alias: wildcard.alias,
            path: wildcard.path,
            isWildcardRef: wildcard.isWildcardRef,
            materializationName: wildcard.name,
            schemaName: wildcard.schemaName,
            databaseName: wildcard.databaseName,
            warehouseName: wildcard.warehouseName,
          };

        const wildcardToFix = wildcard;

        const materializationRef = this.#getBestMatchingMaterialization(
          wildcardToFix.path,
          element.materializations,
          wildcard.name,
          catalog
        );

        return {
          dependencyType: wildcardToFix.dependencyType,
          name: wildcardToFix.name,
          alias: wildcardToFix.alias,
          path: wildcardToFix.path,
          isWildcardRef: wildcardToFix.isWildcardRef,
          materializationName: materializationRef.name,
          schemaName: materializationRef.schemaName,
          databaseName: materializationRef.databaseName,
          warehouseName: materializationRef.warehouseName,
        };
      });

      return { materializations: element.materializations, columns, wildcards };
    });

    return fixedStatementRefs;
  };

  /* Runs through tree of parsed logic and extract all refs of materializations and columns (self and parent materializations and columns) */
  static #getStatementRefs = (
    fileObj: any,
    catalog: CatalogModelData[]
  ): Refs[] => {
    const statementRefsPrototype: RefsPrototype[] = [];

    if (
      fileObj.constructor === Object &&
      fileObj[SQLElement.STATEMENT] !== undefined
    ) {
      const statementRefsObj = this.#extractRefs(fileObj[SQLElement.STATEMENT]);
      statementRefsPrototype.push(statementRefsObj);
    } else if (Object.prototype.toString.call(fileObj) === '[object Array]') {
      const statementObjects = fileObj.filter(
        (statement: any) => SQLElement.STATEMENT in statement
      );

      statementObjects.forEach((statement: any) => {
        const statementRefsObj = this.#extractRefs(
          statement[SQLElement.STATEMENT]
        );
        statementRefsPrototype.push(statementRefsObj);
      });
    }
    statementRefsPrototype.forEach((prototype) => {
      prototype.columns.forEach((val, index) => {
        const nextCol = prototype.columns[index + 1];

        if (val.name.includes('$')) {
          const columnNumber = val.name.split('$')[1];
          const materialisationNames = prototype.materializations.map((mat) => (mat.name));

          let materialisation: string;

          if (materialisationNames.length === 1)
            [materialisation] = materialisationNames;
          else
            materialisation = val.materializationName ? val.materializationName : '';
            // eslint-disable-next-line no-param-reassign
            val.dependencyType = DependencyType.DATA;

          const filteredCatalog = catalog.filter((model) => 
            materialisation && model.materialisationName === materialisation.toUpperCase()
          );
          const [realName] = filteredCatalog.map((model) => 
            model.columnNames[parseInt(columnNumber, 10) - 1]
          );

          if (realName)
            // eslint-disable-next-line no-param-reassign
            val.alias = realName;

        }

        if (!nextCol) return;
        if (
          val.dependencyType === DependencyType.DEFINITION &&
          nextCol.dependencyType === DependencyType.DATA
        )
          nextCol.alias = val.name;

      });
    });

    return this.#buildStatementRefs(statementRefsPrototype, catalog);
  };

  static create = (prototype: LogicPrototype): Logic => {
    if (!prototype.id) throw new TypeError('Logic must have id');
    if (!prototype.dbtModelId)
      throw new TypeError('Logic must have dbtModelId');
    if (!prototype.parsedLogic)
      throw new TypeError('Logic creation requires parsed SQL logic');
    if (!prototype.lineageId) throw new TypeError('Logic must have lineageId');
    if (!prototype.catalog) throw new TypeError('Logic must have catalog data');

    const parsedLogicObj = JSON.parse(prototype.parsedLogic);

    const statementRefs = this.#getStatementRefs(
      parsedLogicObj.file,
      prototype.catalog
    );

    const logic = this.build({
      id: prototype.id,
      dbtModelId: prototype.dbtModelId,
      parsedLogic: prototype.parsedLogic,
      statementRefs,
      lineageId: prototype.lineageId,
    });

    return logic;
  };

  static build = (properties: LogicProperties): Logic => {
    if (!properties.id) throw new TypeError('Logic must have id');
    if (!properties.dbtModelId)
      throw new TypeError('Logic must have dbtModelId');
    if (!properties.parsedLogic)
      throw new TypeError('Logic creation requires parsed SQL logic');
    if (!properties.lineageId) throw new TypeError('Logic must have lineageId');

    const logic = new Logic({
      id: properties.id,
      dbtModelId: properties.dbtModelId,
      parsedLogic: properties.parsedLogic,
      statementRefs: properties.statementRefs,
      lineageId: properties.lineageId,
    });

    return logic;
  };
}
