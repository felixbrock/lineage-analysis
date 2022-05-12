import { DependencyType } from './dependency';
import SQLElement from '../value-types/sql-element';

export interface LogicProperties {
  id: string;
  dbtModelId: string;
  sql: string;
  parsedLogic: string;
  statementRefs: Refs;
  lineageId: string;
}

export interface LogicPrototype {
  id: string;
  dbtModelId: string;
  sql: string;
  parsedLogic: string;
  lineageId: string;
  catalog: CatalogModelData[];
}

export interface CatalogModelData {
  modelName: string;
  materializationName: string;
  columnNames: string[];
}

interface TransientMatRepresentation {
  representativeName: string;
  representativeAlias?: string;
  representedName: string;
}

interface RefContext {
  path: string;
  location: string;
}

interface Ref {
  name: string;
  alias?: string;
  schemaName?: string;
  databaseName?: string;
  warehouseName?: string;
}

export interface MaterializationRefPrototype extends Ref {
  contexts: RefContext[];
}

type MaterializationType = 'self' | 'transient' | 'dependency';

export interface MaterializationRef extends MaterializationRefPrototype {
  type: MaterializationType;
}

interface ColumnRefPrototype extends Ref {
  dependencyType: DependencyType;
  isWildcardRef: boolean;
  materializationName?: string;
  context: RefContext;
}

export interface ColumnRef
  extends Omit<ColumnRefPrototype, 'materializationName'> {
  materializationName: string;
}

interface Alias {
  key: string;
  value: string;
  refPath: string;
}

interface TempExtractionData {
  unmatchedAliases?: Alias;
}

interface RefsExtractionDto {
  temp: TempExtractionData;
  refsPrototype: RefsPrototype;
}

interface RefsPrototype {
  materializations: MaterializationRefPrototype[];
  columns: ColumnRefPrototype[];
  wildcards: ColumnRefPrototype[];
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
  path: string;
  contextLocation: string;
}

export class Logic {
  #id: string;

  #dbtModelId: string;

  #sql: string;

  #parsedLogic: string;

  #statementRefs: Refs;

  #lineageId: string;

  get id(): string {
    return this.#id;
  }

  get dbtModelId(): string {
    return this.#dbtModelId;
  }

  get sql(): string {
    return this.#sql;
  }

  get parsedLogic(): string {
    return this.#parsedLogic;
  }

  get statementRefs(): Refs {
    return this.#statementRefs;
  }

  get lineageId(): string {
    return this.#lineageId;
  }

  private constructor(properties: LogicProperties) {
    this.#id = properties.id;
    this.#dbtModelId = properties.dbtModelId;
    this.#sql = properties.sql;
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

  // /* Checks if a table ref is describing the resulting materialization of an corresponding SQL model */
  // static #isSelfRef = (path: string): boolean => {
  //   const selfElements = [
  //     `${SQLElement.CREATE_TABLE_STATEMENT}.${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`,
  //     `${SQLElement.FROM_EXPRESSION_ELEMENT}.${SQLElement.TABLE_EXPRESSION}.${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`,
  //     `${SQLElement.COMMON_TABLE_EXPRESSION}`,
  //   ];
  //   const nonSelfElements = [`${SQLElement.FROM_EXPRESSION_ELEMENT}`];

  //   return (
  //     selfElements.some((element) => path.includes(element)) &&
  //     nonSelfElements.every((element) => !path.includes(element))
  //   );
  // };

  /* Returns the type of dependency identified in the SQL logic */
  static #getColumnDependencyType = (path: string): DependencyType => {
    const definitionElements = [
      `${SQLElement.COLUMN_DEFINITION}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.ALIAS_EXPRESSION}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.BARE_FUNCTION}`,
      `${SQLElement.LITERAL}`,
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
    const path = this.#appendPath(props.key, props.path);

    return {
      dependencyType: this.#getColumnDependencyType(path),
      alias: props.alias,
      name: columnValueRef.columnName,
      materializationName: columnValueRef.materializationName,
      schemaName: columnValueRef.schemaName,
      databaseName: columnValueRef.databaseName,
      warehouseName: columnValueRef.warehouseName,
      isWildcardRef: false,
      context: { path, location: props.contextLocation },
    };
  };

  /* Handles any materialization ref found in the parsed SQL logic */
  static #handleMaterializationIdentifierRef = (
    props: HandlerProperties<string>
  ): MaterializationRefPrototype => {
    const materializationValueRef = this.#splitMaterializationValue(
      props.value
    );
    const path = this.#appendPath(props.key, props.path);

    return {
      alias: props.alias,
      name: materializationValueRef.materializationName,
      schemaName: materializationValueRef.schemaName,
      databaseName: materializationValueRef.databaseName,
      warehouseName: materializationValueRef.warehouseName,
      contexts: [{ path, location: props.contextLocation }],
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

    const path = this.#appendPath(props.key, props.path);

    if (isDotNoation)
      return {
        dependencyType: this.#getColumnDependencyType(path),
        name: SQLElement.WILDCARD_IDENTIFIER_STAR,
        materializationName:
          props.value[SQLElement.WILDCARD_IDENTIFIER_IDENTIFIER],
        isWildcardRef: true,
        context: { path, location: props.contextLocation },
      };
    if (
      wildcardElementKeys.length === 1 &&
      wildcardElementKeys.includes(SQLElement.WILDCARD_IDENTIFIER_STAR)
    )
      return {
        dependencyType: this.#getColumnDependencyType(path),
        name: SQLElement.WILDCARD_IDENTIFIER_STAR,
        isWildcardRef: true,
        context: { path, location: props.contextLocation },
      };
    throw new SyntaxError('Unhandled wildcard-dict use-case');
  };

  static #sanitizeValue = (value: string): string =>
    value.replace(/["'`]/g, '');

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
      materializationName: valueElements[0]
        ? this.#sanitizeValue(valueElements[0])
        : valueElements[0],
      schemaName: valueElements[1]
        ? this.#sanitizeValue(valueElements[1])
        : valueElements[1],
      databaseName: valueElements[2]
        ? this.#sanitizeValue(valueElements[2])
        : valueElements[2],
      warehouseName: valueElements[3]
        ? this.#sanitizeValue(valueElements[3])
        : valueElements[3],
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
      columnName: valueElements[0]
        ? this.#sanitizeValue(valueElements[0])
        : valueElements[0],
      materializationName: valueElements[1]
        ? this.#sanitizeValue(valueElements[1])
        : valueElements[1],
      schemaName: valueElements[2]
        ? this.#sanitizeValue(valueElements[2])
        : valueElements[2],
      databaseName: valueElements[3]
        ? this.#sanitizeValue(valueElements[3])
        : valueElements[3],
      warehouseName: valueElements[4]
        ? this.#sanitizeValue(valueElements[4])
        : valueElements[4],
    };
  };

  /* Adds a new materialization reference found safely to the existing array of already identified refs */
  static #pushMaterialization = (
    ref: MaterializationRefPrototype,
    refs: MaterializationRefPrototype[]
  ): MaterializationRefPrototype[] => {
    // todo - ignores the case .schema1.tableName and .schema2.tableName where both tables have the same name. Valid case, but logic will throw an error

    const optionShowsExistence = (first?: string, second?: string): boolean =>
      !first || !second || first === second;

    const findExistingInstance = (
      element: MaterializationRefPrototype
    ): boolean =>
      element.name === ref.name &&
      optionShowsExistence(element.schemaName, ref.schemaName) &&
      optionShowsExistence(element.databaseName, ref.databaseName) &&
      optionShowsExistence(element.warehouseName, ref.warehouseName);

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

    existingInstance.contexts.push(...ref.contexts);

    return refs.map((element) =>
      findExistingInstance(element) ? existingInstance : element
    );
  };

  /* Checks if an alias contains information */
  static #aliasExists = (alias: Alias): boolean =>
    !!(alias.key || alias.value || alias.refPath);

  /* In case of an unassigned alias merge refs and subrefs in a special way */
  static #mergeRefs = (
    refsPrototype: RefsPrototype,
    subExtractionDto: RefsExtractionDto,
    alias: Alias
  ): RefsExtractionDto => {
    const newRefsPrototype = refsPrototype;

    const numberOfColumns = subExtractionDto.refsPrototype.columns.length;
    const numberOfWildcards = subExtractionDto.refsPrototype.wildcards.length;
    const numberOfMaterializations =
      subExtractionDto.refsPrototype.materializations.length;

    if (!this.#aliasExists(alias)) {
      newRefsPrototype.columns.push(...subExtractionDto.refsPrototype.columns);
      subExtractionDto.refsPrototype.materializations.forEach(
        (materialization) => {
          newRefsPrototype.materializations = this.#pushMaterialization(
            materialization,
            newRefsPrototype.materializations
          );
        }
      );
      newRefsPrototype.wildcards.push(
        ...subExtractionDto.refsPrototype.wildcards
      );

      return {
        refsPrototype: newRefsPrototype,
        temp: { unmatchedAliases: subExtractionDto.temp.unmatchedAliases },
      };
    }

    if (subExtractionDto.temp.unmatchedAliases)
      throw new ReferenceError(
        'Two unmatched aliases at the same time. Unable to assign'
      );
    if (numberOfWildcards > 1)
      throw new ReferenceError(
        'Multiple wildcards found that potentially use alias'
      );
    if (numberOfColumns >= 1 && numberOfWildcards >= 1)
      throw new ReferenceError(
        'Columns and wildcards found that potentially use alias'
      );
    if (numberOfMaterializations > 1)
      throw new ReferenceError(
        'Multiple materializations found that potentially use alias'
      );

    if (numberOfColumns === 1) {
      const column = subExtractionDto.refsPrototype.columns[0];
      column.alias = alias.value;

      newRefsPrototype.columns.push(column);

      subExtractionDto.refsPrototype.materializations.forEach(
        (materialization) => {
          newRefsPrototype.materializations = this.#pushMaterialization(
            materialization,
            newRefsPrototype.materializations
          );
        }
      );

      return { refsPrototype: newRefsPrototype, temp: {} };
    }
    if (numberOfColumns > 1) {
      const { columns } = subExtractionDto.refsPrototype;

      columns.forEach((element) => {
        const column = element;
        column.alias = alias.value;

        newRefsPrototype.columns.push(column);
      });

      subExtractionDto.refsPrototype.materializations.forEach(
        (materialization) => {
          newRefsPrototype.materializations = this.#pushMaterialization(
            materialization,
            newRefsPrototype.materializations
          );
        }
      );

      return { refsPrototype: newRefsPrototype, temp: {} };
    }
    if (numberOfWildcards === 1) {
      const wildcard = subExtractionDto.refsPrototype.wildcards[0];
      wildcard.alias = alias.value;

      newRefsPrototype.wildcards.push(wildcard);

      subExtractionDto.refsPrototype.materializations.forEach(
        (materialization) => {
          newRefsPrototype.materializations = this.#pushMaterialization(
            materialization,
            newRefsPrototype.materializations
          );
        }
      );

      return { refsPrototype: newRefsPrototype, temp: {} };
    }
    if (numberOfMaterializations === 1) {
      const materialization =
        subExtractionDto.refsPrototype.materializations[0];
      materialization.alias = alias.value;

      newRefsPrototype.materializations.push(materialization);

      return { refsPrototype: newRefsPrototype, temp: {} };
    }

    return {
      refsPrototype: newRefsPrototype,
      temp: { unmatchedAliases: alias },
    };
  };

  /* Merges multiple WITH CTEs into one RefsPrototype by creating unified context */
  static #mergeWithRefsPrototypes = (
    prototypes: RefsPrototype[]
  ): RefsPrototype => {
    const refsPrototype: RefsPrototype = {
      materializations: [],
      columns: [],
      wildcards: [],
    };

    const materializations = prototypes
      .map((element) => element.materializations)
      .flat();

    materializations.forEach((materialization) => {
      refsPrototype.materializations = this.#pushMaterialization(
        materialization,
        refsPrototype.materializations
      );
    });

    refsPrototype.columns = prototypes.map((element) => element.columns).flat();

    refsPrototype.wildcards = prototypes
      .map((element) => element.wildcards)
      .flat();

    return refsPrototype;
  };

  // };

  /* Directly interacts with parsed SQL logic. Calls different handlers based on use-case. 
  Runs through parse SQL logic (JSON object) and check for potential dependencies. */
  static #extractRefs = (
    parsedSQL: { [key: string]: any },
    contextLocation: string,
    path = '',
    recursionLevel = 0
  ): RefsExtractionDto => {
    let refPath = path;

    let refsPrototype: RefsPrototype = {
      columns: [],
      materializations: [],
      wildcards: [],
    };

    const temp: TempExtractionData = {};

    const valueKeyRepresentatives = [SQLElement.KEYWORD_AS];

    const aliasToColumnDefinitionElements = [
      SQLElement.LITERAL,
      SQLElement.BARE_FUNCTION,
    ];

    let alias: Alias = { key: '', value: '', refPath: '' };
    Object.entries(parsedSQL).forEach((parsedSQLElement) => {
      const key = parsedSQLElement[0];

      const value =
        typeof parsedSQLElement[1] === 'string' &&
        !valueKeyRepresentatives.includes(parsedSQLElement[1])
          ? parsedSQLElement[1].toUpperCase()
          : parsedSQLElement[1];

      refPath =
        key === SQLElement.KEYWORD && valueKeyRepresentatives.includes(value)
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
              path: refPath,
              alias: alias.value,
              contextLocation,
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
            path: refPath,
            alias: alias.value,
            contextLocation,
          });

          alias = { key: '', value: '', refPath: '' };

          refsPrototype.materializations = this.#pushMaterialization(
            ref,
            refsPrototype.materializations
          );
        }
      } else if (key === SQLElement.WILDCARD_IDENTIFIER)
        refsPrototype.wildcards.push(
          this.#handleWildCardRef({
            key,
            value,
            path: refPath,
            contextLocation,
          })
        );
      else if (
        this.#aliasExists(alias) &&
        aliasToColumnDefinitionElements.includes(key) &&
        typeof value === 'string'
      ) {
        refsPrototype.columns.push(
          this.#handleColumnIdentifierRef({
            key,
            value,
            path: refPath,
            alias: alias.value,
            contextLocation,
          })
        );

        alias = { key: '', value: '', refPath: '' };
      } else if (value.constructor === Object) {
        const subExtractionDto = this.#extractRefs(
          value,
          this.#appendPath(`${0}`, contextLocation),
          this.#appendPath(key, refPath),
          recursionLevel + 1
        );

        const mergeExtractionDto = this.#mergeRefs(
          refsPrototype,
          subExtractionDto,
          alias
        );

        refsPrototype = mergeExtractionDto.refsPrototype;

        if (mergeExtractionDto.temp.unmatchedAliases)
          alias = mergeExtractionDto.temp.unmatchedAliases;
        else alias = { key: '', value: '', refPath: '' };
      } else if (Object.prototype.toString.call(value) === '[object Array]') {
        if (key === SQLElement.COLUMN_REFERENCE) {
          refsPrototype.columns.push(
            this.#handleColumnIdentifierRef({
              key,
              value: this.#joinArrayValue(value),
              path: refPath,
              alias: alias.value,
              contextLocation,
            })
          );

          alias = { key: '', value: '', refPath: '' };
        } else if (key === SQLElement.TABLE_REFERENCE) {
          const ref = this.#handleMaterializationIdentifierRef({
            key,
            value: this.#joinArrayValue(value),
            path: refPath,
            alias: alias.value,
            contextLocation,
          });

          refsPrototype.materializations = this.#pushMaterialization(
            ref,
            refsPrototype.materializations
          );

          alias = { key: '', value: '', refPath: '' };
        } else if (key === SQLElement.WITH_COMPOUND_STATEMENT) {
          const withElements: RefsExtractionDto[] = value.map(
            (element: { [key: string]: any }, index: number) =>
              this.#extractRefs(
                element,
                this.#appendPath(`${index}`, contextLocation),
                this.#appendPath(key, refPath),
                recursionLevel + 1
              )
          );

          if (withElements.some((element) => element.temp.unmatchedAliases))
            throw new ReferenceError(
              'Unhandled Case: Unmatched aliases on WITH level'
            );

          const withElementsMerged = this.#mergeWithRefsPrototypes(
            withElements.map((element) => element.refsPrototype)
          );

          const mergeExtractionDto = this.#mergeRefs(
            refsPrototype,
            { refsPrototype: withElementsMerged, temp: {} },
            alias
          );

          refsPrototype = mergeExtractionDto.refsPrototype;
        } else
          value.forEach((element: { [key: string]: any }, index: number) => {
            const subExtractionDto = this.#extractRefs(
              element,
              this.#appendPath(`${index}`, contextLocation),
              this.#appendPath(key, refPath),
              recursionLevel + 1
            );

            const mergeExtractionDto = this.#mergeRefs(
              refsPrototype,
              subExtractionDto,
              alias
            );

            refsPrototype = mergeExtractionDto.refsPrototype;
            if (mergeExtractionDto.temp.unmatchedAliases)
              alias = mergeExtractionDto.temp.unmatchedAliases;
            else alias = { key: '', value: '', refPath: '' };
          });
      }
    });

    if (recursionLevel === 0 && alias.value)
      refsPrototype.columns.push(
        this.#handleColumnIdentifierRef({
          key: alias.key,
          value: alias.value,
          path: alias.refPath,
          contextLocation,
        })
      );
    else if (alias.value) temp.unmatchedAliases = alias;

    return { temp, refsPrototype };
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
      materialization.contexts.forEach((context) => {
        const materializationPathElements = context.path.split('.');

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
              modelData.materializationName === materializationName &&
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

  /* Transforming prototype that is representing the analyzed model itself (self) to materializationRef, 
  by improving information coverage on object level  */
  static #buildSelfMaterializationRef = (
    matRefPrototypes: MaterializationRefPrototype[]
  ): MaterializationRef => {
    const lastSelectMatRefPosition = matRefPrototypes
      .map((materialization) => {
        const selfMatQualifiers = [
          `${SQLElement.CREATE_TABLE_STATEMENT}.${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`,
          `${SQLElement.WITH_COMPOUND_STATEMENT}.${SQLElement.COMMON_TABLE_EXPRESSION}.${SQLElement.IDENTIFIER}`,
        ];
        const selfMatSelectQualifier = `${SQLElement.FROM_EXPRESSION_ELEMENT}.${SQLElement.TABLE_EXPRESSION}.${SQLElement.TABLE_REFERENCE}`;
        const selectContexts = materialization.contexts.filter(
          (context) =>
            context.path.includes(selfMatSelectQualifier) ||
            selfMatQualifiers.some((qualifier) =>
              context.path.includes(qualifier)
            )
        );
        return selectContexts
          .map((context) => context.location)
          .sort()
          .reverse()[0];
      })
      .sort()
      .reverse()[0];

    const selfMatPrototype = matRefPrototypes.find((materialization) => {
      const context = materialization.contexts.filter(
        (element) => element.location === lastSelectMatRefPosition
      );

      if (!context.length) return false;

      if (context.length > 1)
        throw new SyntaxError(
          'Context location duplication occured. Incorrect logic in place'
        );

      return true;
    });

    if (!selfMatPrototype)
      throw new ReferenceError('Self materialization was not identified');

    return { ...selfMatPrototype, type: 'self' };
  };

  /* Transforming prototypes that are not representing the analyzed model itself (non-self) to materializationRefs, by improving information coverage on object level  */
  static #buildNonSelfMaterializationRefs = (
    matRefPrototypes: MaterializationRefPrototype[]
  ): MaterializationRef[] => {
    const transientQualifier = `${SQLElement.WITH_COMPOUND_STATEMENT}.${SQLElement.COMMON_TABLE_EXPRESSION}.${SQLElement.IDENTIFIER}`;

    const materializations: MaterializationRef[] = matRefPrototypes.map(
      (materialization): MaterializationRef => {
        const paths = materialization.contexts.map((element) => element.path);

        if (paths.includes(transientQualifier))
          return {
            ...materialization,
            type: 'transient',
          };

        return {
          ...materialization,
          type: 'dependency',
        };
      }
    );

    return materializations;
  };

  /* Return the name of the materialization that is represented by a referenced transient materialization */
  static #getRepresentedMatName = (
    materializationName: string,
    transientMatRepresentations: TransientMatRepresentation[]
  ): string => {
    const representations = transientMatRepresentations.filter(
      (representation) =>
        representation.representativeName === materializationName ||
        (representation.representativeAlias &&
          representation.representativeAlias === materializationName)
    );

    if (representations.length > 1)
      throw new RangeError(
        'Multiple transientRepresentations found for materialization name'
      );

    return representations.length
      ? representations[0].representativeName
      : materializationName;
  };

  /* Transforming prototypes to columnRefs, by improving information coverage on object level  */
  static #buildColumnRefs = (
    columnRefPrototypes: ColumnRefPrototype[],
    nonSelfMaterializationRefs: MaterializationRef[],
    selfMaterializationRef: MaterializationRef,
    catalog: CatalogModelData[]
  ): ColumnRef[] => {
    const columns: ColumnRef[] = columnRefPrototypes.map((column) => {
      const transientMatRepresentations = this.#getTransientRepresentations(
        nonSelfMaterializationRefs
      );

      if (column.materializationName) {
        const materializationName = this.#getRepresentedMatName(
          column.materializationName,
          transientMatRepresentations
        );

        return {
          ...column,
          materializationName,
        };
      }

      const materializations = nonSelfMaterializationRefs.concat(
        selfMaterializationRef
      );

      const materializationRef = this.#getBestMatchingMaterialization(
        column.context.path,
        materializations,
        column.name,
        catalog
      );

      const materializationName = this.#getRepresentedMatName(
        materializationRef.name,
        transientMatRepresentations
      );

      return {
        ...column,
        materializationName,
        schemaName: materializationRef.schemaName,
        databaseName: materializationRef.databaseName,
        warehouseName: materializationRef.warehouseName,
      };
    });

    return columns;
  };

  /* Identify transient materializations that only exists at execution time and merge link them with the represented tables */
  static #getTransientRepresentations = (
    nonSelfMaterializationRefs: MaterializationRef[]
  ): TransientMatRepresentation[] => {
    const transientQualifier = `${SQLElement.WITH_COMPOUND_STATEMENT}.${SQLElement.COMMON_TABLE_EXPRESSION}.${SQLElement.IDENTIFIER}`;

    const transientMaterializationRefs = nonSelfMaterializationRefs.filter(
      (ref) => ref.type === 'transient'
    );

    const representations: TransientMatRepresentation[] =
      transientMaterializationRefs.map((ref) => {
        const definitionContext = ref.contexts.find((context) =>
          context.path.includes(transientQualifier)
        );
        if (!definitionContext)
          throw new ReferenceError(
            'Transient context of transient materialization not found'
          );

        const representedOnes = nonSelfMaterializationRefs.filter((element) =>
          element.contexts.filter((context) =>
            context.location.startsWith(definitionContext.location)
          )
        );

        if (representedOnes.length > 1)
          throw new Error(
            'Unhandled case of WITH materialization representation'
          );

        return {
          representativeName: ref.name,
          representativeAlias: ref.alias,
          representedName: representedOnes[0].name,
        };
      });

    return representations;
  };

  /* Transforms RefsPrototype object to Refs object by identifying missing materialization refs */
  static #buildStatementRefs = (
    refsPrototype: RefsPrototype,
    catalog: CatalogModelData[]
  ): Refs => {
    const selfMaterializationRef = this.#buildSelfMaterializationRef(
      refsPrototype.materializations
    );

    const nonSelfMatRefsPrototypes = refsPrototype.materializations.filter(
      (materialization) => {
        const contextLocations = materialization.contexts.map(
          (context) => context.location
        );
        const selfMatRefLocations = selfMaterializationRef.contexts.map(
          (context) => context.location
        );
        return contextLocations.some((location) =>
          selfMatRefLocations.includes(location)
        );
      }
    );

    const nonSelfMaterializationRefs = this.#buildNonSelfMaterializationRefs(
      nonSelfMatRefsPrototypes
    );

    const materializations = nonSelfMaterializationRefs.concat(
      selfMaterializationRef
    );

    const columns = this.#buildColumnRefs(
      refsPrototype.columns,
      nonSelfMaterializationRefs,
      selfMaterializationRef,
      catalog
    );

    const wildcards = this.#buildColumnRefs(
      refsPrototype.wildcards,
      nonSelfMaterializationRefs,
      selfMaterializationRef,
      catalog
    );

    return { materializations, columns, wildcards };
  };

  /* Runs through tree of parsed logic and extract all refs of materializations and columns (self and parent materializations and columns) */
  static #getStatementRefs = (
    fileObj: any,
    catalog: CatalogModelData[]
  ): Refs => {
    const statementRefsPrototype: RefsPrototype = {
      materializations: [],
      columns: [],
      wildcards: [],
    };

    if (
      fileObj.constructor === Object &&
      fileObj[SQLElement.STATEMENT] !== undefined
    ) {
      const statementExtractionDto = this.#extractRefs(
        fileObj[SQLElement.STATEMENT],
        '0'
      );

      statementRefsPrototype.materializations.push(
        ...statementExtractionDto.refsPrototype.materializations
      );
      statementRefsPrototype.columns.push(
        ...statementExtractionDto.refsPrototype.columns
      );
      statementRefsPrototype.wildcards.push(
        ...statementExtractionDto.refsPrototype.wildcards
      );
    } else if (Object.prototype.toString.call(fileObj) === '[object Array]') {
      const statementObjects = fileObj.filter(
        (statement: any) => SQLElement.STATEMENT in statement
      );

      statementObjects.forEach((statement: any, index: number) => {
        const statementExtractionDto = this.#extractRefs(
          statement[SQLElement.STATEMENT],
          `${index}`
        );
        statementRefsPrototype.materializations.push(
          ...statementExtractionDto.refsPrototype.materializations
        );
        statementRefsPrototype.columns.push(
          ...statementExtractionDto.refsPrototype.columns
        );
        statementRefsPrototype.wildcards.push(
          ...statementExtractionDto.refsPrototype.wildcards
        );
      });
    }

    statementRefsPrototype.columns.forEach((column, index) => {
      const nextCol = statementRefsPrototype.columns[index + 1];
      const thisCol = column;

      if (column.name.includes('$')) {
        const columnNumber = column.name.split('$')[1];
        const materializationNames =
          statementRefsPrototype.materializations.map((mat) => mat.name);

        let materialization: string;

        if (materializationNames.length === 1)
          [materialization] = materializationNames;
        else
          materialization = column.materializationName
            ? column.materializationName
            : '';
        thisCol.dependencyType = DependencyType.DATA;

        const filteredCatalog = catalog.filter(
          (model) =>
            materialization &&
            model.materializationName === materialization.toUpperCase()
        );
        const [realName] = filteredCatalog.map(
          (model) => model.columnNames[parseInt(columnNumber, 10) - 1]
        );

        if (realName) thisCol.alias = realName;
      }

      if (!nextCol) return;
      if (
        column.dependencyType === DependencyType.DEFINITION &&
        nextCol.dependencyType === DependencyType.DATA
      )
        nextCol.alias = column.name;
    });

    return this.#buildStatementRefs(statementRefsPrototype, catalog);
  };

  static create = (prototype: LogicPrototype): Logic => {
    if (!prototype.id) throw new TypeError('Logic must have id');
    if (!prototype.dbtModelId)
      throw new TypeError('Logic must have dbtModelId');
    if (!prototype.sql)
      throw new TypeError('Logic creation requires SQL logic');
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
      sql: prototype.sql,
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
      sql: properties.sql,
      parsedLogic: properties.parsedLogic,
      statementRefs: properties.statementRefs,
      lineageId: properties.lineageId,
    });

    return logic;
  };
}
