import { DependencyType } from './dependency';
import SQLElement from '../value-types/sql-element';

export interface MaterializationDefinition {
  dbtModelId: string;
  materializationName: string;
  schemaName?: string;
  databaseName?: string;
}

export interface LogicProperties {
  id: string;
  dbtModelId: string;
  sql: string;
  dependentOn: MaterializationDefinition[];
  parsedLogic: string;
  statementRefs: Refs;
  lineageId: string;
}

export interface LogicPrototype {
  id: string;
  dbtModelId: string;
  modelName: string;
  sql: string;
  dependentOn: MaterializationDefinition[];
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
  representedSchemaName?: string;
  representedDatabaseName?: string;
  representedWarehouseName?: string;
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
  boundedContext: string;
  isUsed: boolean;
}

interface TempExtractionData {
  alias?: Alias;
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

interface ExtractRefProperties {
  key: string,
  alias?: Alias,
  value: any,
  refPath: string,
  refsPrototype: RefsPrototype,
  contextLocation: string,
  recursionLevel: number,
  path?: string,
}

interface HandlerReturn {
  newAlias?: Alias,
  newPrototype: RefsPrototype,
}


export class Logic {
  #id: string;

  #dbtModelId: string;

  #sql: string;

  #dependentOn: MaterializationDefinition[];

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

  get dependentOn(): MaterializationDefinition[] {
    return this.#dependentOn;
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
    this.#dependentOn = properties.dependentOn;
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

  /* Returns the type of dependency identified in the SQL logic */
  static #getColumnDependencyType = (path: string): DependencyType => {
    const definitionElements = [
      `${SQLElement.COLUMN_DEFINITION}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.ALIAS_EXPRESSION}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.BARE_FUNCTION}`,
      `${SQLElement.LITERAL}`,
    ];
    
    const dataDependencyElementsRegex: RegExp[] = [
      new RegExp(
        `${SQLElement.SELECT_CLAUSE_ELEMENT}.*${SQLElement.COLUMN_REFERENCE}.${SQLElement.IDENTIFIER}`
      ),
      new RegExp(
        `${SQLElement.SELECT_CLAUSE_ELEMENT}.*${SQLElement.WILDCARD_EXPRESSION}.${SQLElement.WILDCARD_IDENTIFIER}`
      ),
      new RegExp(
        `${SQLElement.FUNCTION}.*${SQLElement.COLUMN_REFERENCE}.${SQLElement.IDENTIFIER}`
      ),
    ];


    if (definitionElements.some((element) => path.includes(element)))
      return DependencyType.DEFINITION;
    if (dataDependencyElementsRegex.some((element) => !!path.match(element)))
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
  static #handleColumnRef = (
    props: HandlerProperties<string>
  ): ColumnRefPrototype => {
    const columnValueRef = this.#splitColumnValue(props.value);

    const toAppend =
      props.key === SQLElement.IDENTIFIER
        ? props.key
        : `${props.key}.${SQLElement.IDENTIFIER}`;
    const path = this.#appendPath(toAppend, props.path);

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
  static #handleMaterializationRef = (
    props: HandlerProperties<string>
  ): MaterializationRefPrototype => {
    const materializationValueRef = this.#splitMaterializationValue(
      props.value
    );

    const toAppend =
      props.key === SQLElement.IDENTIFIER
        ? props.key
        : `${props.key}.${SQLElement.IDENTIFIER}`;
    const path = this.#appendPath(toAppend, props.path);

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

  static #sanitizeValue = (value: string): string => {
    if (!value) return value;
    return value.replace(/["'`]/g, '');
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
      materializationName: this.#sanitizeValue(valueElements[0]),
      schemaName: this.#sanitizeValue(valueElements[1]),
      databaseName: this.#sanitizeValue(valueElements[2]),
      warehouseName: this.#sanitizeValue(valueElements[3]),
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
      columnName: this.#sanitizeValue(valueElements[0]),
      materializationName: this.#sanitizeValue(valueElements[1]),
      schemaName: this.#sanitizeValue(valueElements[2]),
      databaseName: this.#sanitizeValue(valueElements[3]),
      warehouseName: this.#sanitizeValue(valueElements[4]),
    };
  };

  /* Adds a new materialization reference to the existing array of already identified refs in a secure manner */
  static #pushMaterialization = (
    ref: MaterializationRefPrototype,
    refs: MaterializationRefPrototype[]
  ): MaterializationRefPrototype[] => {
    // todo - ignores the case .schema1.tableName and .schema2.tableName where both tables have the same name. Valid case, but logic will throw an error

    // Due to incomplete nature of prototypes false is only returned in case both values are defined, but differ
    const valuesAreEqual = (first?: string, second?: string): boolean =>
      !first || !second || first === second;

    const findExistingInstance = (
      element: MaterializationRefPrototype
    ): boolean =>
      element.name === ref.name &&
      valuesAreEqual(element.schemaName, ref.schemaName) &&
      valuesAreEqual(element.databaseName, ref.databaseName) &&
      valuesAreEqual(element.warehouseName, ref.warehouseName);

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

  /* In case of an unassigned alias merge refs and subrefs in a special way */
  static #mergeRefs = (
    refsPrototype: RefsPrototype,
    subExtractionDto: RefsExtractionDto,
    alias?: Alias
  ): RefsExtractionDto => {
    const newRefsPrototype = refsPrototype;
    const localAlias = alias;

    const numberOfColumns = subExtractionDto.refsPrototype.columns.length;
    const numberOfWildcards = subExtractionDto.refsPrototype.wildcards.length;
    const numberOfMaterializations =
      subExtractionDto.refsPrototype.materializations.length;

    if (!localAlias) {
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
        temp: { alias: subExtractionDto.temp.alias },
      };
    }

    if (subExtractionDto.temp.alias)
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
      column.alias = localAlias.value;

      newRefsPrototype.columns.push(column);

      subExtractionDto.refsPrototype.materializations.forEach(
        (materialization) => {
          newRefsPrototype.materializations = this.#pushMaterialization(
            materialization,
            newRefsPrototype.materializations
          );
        }
      );
      
      localAlias.isUsed = true;
    } else if (numberOfColumns > 1) {
      const { columns } = subExtractionDto.refsPrototype;

      columns.forEach((element) => {
        const column = element;
        column.alias = localAlias.value;

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

      localAlias.isUsed = true;
    } else if (numberOfWildcards === 1) {
      const wildcard = subExtractionDto.refsPrototype.wildcards[0];
      wildcard.alias = localAlias.value;

      newRefsPrototype.wildcards.push(wildcard);

      subExtractionDto.refsPrototype.materializations.forEach(
        (materialization) => {
          newRefsPrototype.materializations = this.#pushMaterialization(
            materialization,
            newRefsPrototype.materializations
          );
        }
      );

      localAlias.isUsed = true;
    }
    if (numberOfMaterializations === 1) {
      const materialization =
        subExtractionDto.refsPrototype.materializations[0];
      materialization.alias = localAlias.value;

      newRefsPrototype.materializations.push(materialization);

      localAlias.isUsed = true;
    }

    return {
      refsPrototype: newRefsPrototype,
      temp: { alias: localAlias },
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

/* Handles the case where the current key is an identifier */ 
  static #handleIdentifiers = (
    input: ExtractRefProperties
  ): HandlerReturn => {

    if (!input.path) 
      throw new ReferenceError(
        'Path should not be undefined'
      );
    

    const newPrototype = input.refsPrototype;
    let newAlias = input.alias;

    if (input.path.includes(SQLElement.ALIAS_EXPRESSION)) {
      newAlias = { 
        key: input.key,
        value: input.value,
        refPath: input.refPath,
        boundedContext: this.#getContextLocationParent(this.#getContextLocationParent(input.contextLocation)),
        isUsed: false,
       };
    }
    else if (input.path.includes(SQLElement.COLUMN_REFERENCE)) {
      newPrototype.columns.push(
        this.#handleColumnRef({
          key: input.key,
          value: input.value,
          path: input.refPath,
          alias: input.alias ? input.alias.value : undefined,
          contextLocation: input.contextLocation,
        })
      );

      if(newAlias) newAlias.isUsed = true;
    } else if (
      input.path.includes(SQLElement.TABLE_REFERENCE) ||
      (input.path.includes(`${SQLElement.COMMON_TABLE_EXPRESSION}`) &&
        !input.path.includes(`${SQLElement.COMMON_TABLE_EXPRESSION}.`))
    ) {
      const ref = this.#handleMaterializationRef({
        key: input.key,
        value: input.value,
        path: input.refPath,
        alias: input.alias ? input.alias.value : undefined,
        contextLocation: input.contextLocation,
      });

      if(newAlias) newAlias.isUsed = true;
      newPrototype.materializations = this.#pushMaterialization(
        ref,
        newPrototype.materializations
      );
    }
    return { newAlias, newPrototype };
  };

/* Handles the case where the current value is an arbitrary object */
  static #handleValueObject = (
    input: ExtractRefProperties,
    elementIndex: number,
  ): HandlerReturn => {
    if (input.recursionLevel === null)
      throw new ReferenceError(
        'Recursion level should not be undefined'
      );

    let newPrototype = input.refsPrototype;
    let newAlias = input.alias; 

    const subExtractionDto = this.#extractRefs(
      input.value,
      this.#appendPath(`${elementIndex}`, input.contextLocation),
      this.#appendPath(input.key, input.refPath),
      input.recursionLevel + 1
    );

    const mergeExtractionDto = this.#mergeRefs(
      newPrototype,
      subExtractionDto,
      newAlias
    );

    newPrototype = mergeExtractionDto.refsPrototype;

    newAlias = mergeExtractionDto.temp.alias;
    return { newAlias, newPrototype };
  };

/* Handles the case where the current key is a with statement */
  static #handleWithStatement = (
    input: ExtractRefProperties
  ): HandlerReturn => {

    let newPrototype = input.refsPrototype;
    let newAlias = input.alias;

    const withElements: RefsExtractionDto[] = input.value.map(
      (element: { [key: string]: any }, index: number) => 
        this.#extractRefs(
          element,
          this.#appendPath(`${index}`, input.contextLocation),
          this.#appendPath(input.key, input.refPath),
          input.recursionLevel + 1
          )
      
      );

    const withElementsMerged = this.#mergeWithRefsPrototypes(
      withElements.map((element) => element.refsPrototype)
    );

    const mergeExtractionDto = this.#mergeRefs(
      newPrototype,
      { refsPrototype: withElementsMerged, temp: {} },
      input.alias
    );
    newPrototype = mergeExtractionDto.refsPrototype;
    newAlias = mergeExtractionDto.temp.alias;

    return { newAlias, newPrototype };
  };

  /* Directly interacts with parsed SQL logic. Calls different handlers based on use-case. 
  Runs through parse SQL logic (JSON object) and check for potential dependencies. */
  static #extractRefs = (
    parsedSQL: { [key: string]: any },
    contextLocation: string,
    path = '',
    recursionIndex = 0
  ): RefsExtractionDto => {
    let refPath = path;

    let refsPrototype: RefsPrototype = {
      columns: [],
      materializations: [],
      wildcards: [],
    };

    const tempData: TempExtractionData = {};

    const valueKeyRepresentatives = [SQLElement.KEYWORD_AS];

    const aliasToColumnDefinitionElements = [
      SQLElement.LITERAL,
      SQLElement.BARE_FUNCTION,
    ];

    let alias: Alias | undefined;
    Object.entries(parsedSQL).forEach((parsedSQLElement, elementIndex) => {

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
        const { newAlias, newPrototype } = this.#handleIdentifiers({
          key,
          alias,
          value,
          refPath,
          refsPrototype,
          contextLocation,
          path,
          recursionLevel: recursionIndex,
        });

        alias = newAlias;
        refsPrototype = newPrototype;
      }
      else if (key === SQLElement.WILDCARD_IDENTIFIER)
        refsPrototype.wildcards.push(
          this.#handleWildCardRef({
            key,
            value,
            path: refPath,
            contextLocation,
          })
        );
      else if (
        alias &&
        aliasToColumnDefinitionElements.includes(key) &&
        typeof value === 'string'
      ) {
        refsPrototype.columns.push(
          this.#handleColumnRef({
            key,
            value,
            path: refPath,
            alias: alias.value,
            contextLocation,
          })
        );


        if(alias) alias.isUsed = true;
      }
      else if (value.constructor === Object) {
        const { newAlias, newPrototype } = this.#handleValueObject({
          key,
          alias,
          value,
          refPath,
          refsPrototype,
          contextLocation,
          recursionLevel: recursionIndex,
        },
        elementIndex,
        );

        alias = newAlias;
        refsPrototype = newPrototype;
      }

      else if (Object.prototype.toString.call(value) === '[object Array]') {

        if (key === SQLElement.COLUMN_REFERENCE) {
          refsPrototype.columns.push(
            this.#handleColumnRef({
              key,
              value: this.#joinArrayValue(value),
              path: refPath,
              alias: alias ? alias.value : undefined,
              contextLocation,
            })
          );

          if (alias) alias.isUsed = true;
        } else if (key === SQLElement.TABLE_REFERENCE) {
          const ref = this.#handleMaterializationRef({
            key,
            value: this.#joinArrayValue(value),
            path: refPath,
            alias: alias ? alias.value : undefined,
            contextLocation,
          });

          refsPrototype.materializations = this.#pushMaterialization(
            ref,
            refsPrototype.materializations
          );

          if(alias) alias.isUsed = true;
        } else if (key === SQLElement.WITH_COMPOUND_STATEMENT) {
          const { newAlias, newPrototype } = this.#handleWithStatement({
            key,
            alias,
            value,
            refPath,
            refsPrototype,
            contextLocation,
            recursionLevel: recursionIndex,
          });

          alias = newAlias;
          refsPrototype = newPrototype;
        }
        
        else
          value.forEach((element: { [key: string]: any }, index: number) => {
            const subExtractionDto = this.#extractRefs(
              element,
              this.#appendPath(`${index}`, contextLocation),
              this.#appendPath(key, refPath),
              recursionIndex + 1
            );

            const mergeExtractionDto = this.#mergeRefs(
              refsPrototype,
              subExtractionDto,
              alias
            );

            refsPrototype = mergeExtractionDto.refsPrototype;
            alias = mergeExtractionDto.temp.alias;
          });
      }
    });

    if (!alias) return { temp: tempData, refsPrototype };

    const aliasExhausted =
      alias.isUsed &&
      alias.boundedContext === this.#getContextLocationParent(contextLocation);

    const aliasExpired = alias.boundedContext === contextLocation;

    if (recursionIndex === 0)
      refsPrototype.columns.push(
        this.#handleColumnRef({
          key: alias.key,
          value: alias.value,
          path: alias.refPath,
          contextLocation,
        })
      );

    else if (aliasExpired && alias.isUsed) return {temp: tempData, refsPrototype};
    else if (aliasExpired && !alias.isUsed) 
      throw new RangeError('Unmatched alias');
    else if (!aliasExhausted) tempData.alias = alias;

    return { temp: tempData, refsPrototype };
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
    matRefPrototypes: MaterializationRefPrototype[],
    modelName: string
  ): MaterializationRef => {
    const lastSelectPositions = matRefPrototypes.map((materialization) => {
      const selfMatQualifiers = [
        `${SQLElement.CREATE_TABLE_STATEMENT}.${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`,
        `${SQLElement.WITH_COMPOUND_STATEMENT}.${SQLElement.COMMON_TABLE_EXPRESSION}.${SQLElement.IDENTIFIER}`,
      ];
      const selfMatSelectQualifier = `${SQLElement.WITH_COMPOUND_STATEMENT}.${SQLElement.SELECT_STATEMENT}`;

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
    });

    const isPosition = (element: string | undefined): element is string =>
      !!element;

    const lastSelectMatRefPosition = lastSelectPositions
      .filter(isPosition)
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
      return { name: modelName, type: 'self', contexts: [] };

    return { ...selfMatPrototype, name: modelName, type: 'self' };
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
  static #findRepresentedMatName = (
    materializationName: string,
    transientMatRepresentations: TransientMatRepresentation[]
  ): TransientMatRepresentation | undefined => {
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
    if (representations.length) return representations[0];
    return undefined;
  };

  /* Checks if a columnRef represents a column of the currently analyzed materialization (self materialization) */
  static #isSelfRefColumn = (
    prototype: ColumnRef,
    selfMaterializationRef: MaterializationRef
  ): boolean => {
    const nameIsEqual =
      prototype.materializationName === selfMaterializationRef.name ||
      prototype.materializationName === selfMaterializationRef.alias;

    const schemaNameIsEqual = !prototype.schemaName || prototype.schemaName === selfMaterializationRef.schemaName;

    const databaseNameIsEqual =
      !prototype.databaseName ||
      prototype.databaseName === selfMaterializationRef.databaseName;

    const isEqual = nameIsEqual && schemaNameIsEqual && databaseNameIsEqual;

    const wildcardSelfQualifiers = [
      `${SQLElement.WITH_COMPOUND_STATEMENT}.${SQLElement.COMMON_TABLE_EXPRESSION}.${SQLElement.BRACKETED}`,
      `${SQLElement.WITH_COMPOUND_STATEMENT}.${SQLElement.SELECT_STATEMENT}`,
    ];

    if (prototype.isWildcardRef && isEqual)
      return wildcardSelfQualifiers.some((qualifier) =>
        prototype.context.path.includes(qualifier)
      );

    return isEqual;
  };

  /* Transforming prototypes to columnRefs, by improving information coverage on object level  */
  static #buildColumnRefs = (
    columnRefPrototypes: ColumnRefPrototype[],
    nonSelfMaterializationRefs: MaterializationRef[],
    selfMaterializationRef: MaterializationRef,
    catalog: CatalogModelData[]
  ): ColumnRef[] => {
    const columns: ColumnRef[] = columnRefPrototypes.map((prototype) => {
      const transientMatRepresentations = this.#getTransientRepresentations(
        nonSelfMaterializationRefs
      );

      if (prototype.dependencyType === DependencyType.DEFINITION)
        return {
          ...prototype,
          materializationName: selfMaterializationRef.name,
          schemaName: selfMaterializationRef.schemaName,
          databaseName: selfMaterializationRef.databaseName,
          warehouseName: selfMaterializationRef.warehouseName,
        };

      const originalMaterializationName = prototype.materializationName;
      if (originalMaterializationName) {
        const representation = this.#findRepresentedMatName(
          originalMaterializationName,
          transientMatRepresentations
        );

        const columnRef: ColumnRef = {
          ...prototype,
          materializationName: representation
            ? representation.representedName
            : originalMaterializationName,
          schemaName: representation
            ? representation.representedSchemaName
            : prototype.schemaName,
          databaseName: representation
            ? representation.representedDatabaseName
            : prototype.databaseName,
          warehouseName: representation
            ? representation.representedWarehouseName
            : prototype.warehouseName,
        };

        const isSelfRefColumn = this.#isSelfRefColumn(
          columnRef,
          selfMaterializationRef
        );

        if (isSelfRefColumn)
          columnRef.dependencyType = DependencyType.DEFINITION;

        return columnRef;
      }

      const materializations = nonSelfMaterializationRefs.concat(
        selfMaterializationRef
      );

      const materializationRef = this.#getBestMatchingMaterialization(
        prototype.context.path,
        materializations,
        prototype.name,
        catalog
      );

      const representation = this.#findRepresentedMatName(
        materializationRef.name,
        transientMatRepresentations
      );

      const columnRef = {
        ...prototype,
        materializationName: representation
          ? representation.representedName
          : materializationRef.name,
        schemaName: representation
          ? representation.representedSchemaName
          : prototype.schemaName,
        databaseName: representation
          ? representation.representedDatabaseName
          : prototype.databaseName,
        warehouseName: representation
          ? representation.representedWarehouseName
          : prototype.warehouseName,
      };

      const isSelfRefColumn = this.#isSelfRefColumn(
        columnRef,
        selfMaterializationRef
      );

      if (isSelfRefColumn || (columnRef.isWildcardRef && representation))
        columnRef.dependencyType = DependencyType.DEFINITION;

      return columnRef;
    });

    return columns;
  };

  static #getContextLocationParent = (location: string): string =>
    location.slice(0, location.lastIndexOf('.'));

  /* Identify transient materializations that only exists at execution time and link them with the represented tables */
  static #getTransientRepresentations = (
    nonSelfMaterializationRefs: MaterializationRef[]
  ): TransientMatRepresentation[] => {
    const transientQualifier = `${SQLElement.WITH_COMPOUND_STATEMENT}.${SQLElement.COMMON_TABLE_EXPRESSION}.${SQLElement.IDENTIFIER}`;

    const transientMaterializationRefs = nonSelfMaterializationRefs.filter(
      (ref) => ref.type === 'transient'
    );

    const isTransientMatRepresentation = (
      element: TransientMatRepresentation | undefined
    ): element is TransientMatRepresentation => !!element;

    const representations: TransientMatRepresentation[] =
      transientMaterializationRefs
        .map((ref): TransientMatRepresentation | undefined => {
          const definitionContext = ref.contexts.find((context) =>
            context.path.includes(transientQualifier)
          );
          if (!definitionContext)
            throw new ReferenceError(
              'Transient context of transient materialization not found'
            );

          const representedOnes = nonSelfMaterializationRefs.filter(
            (element) => {
              if (element.type === 'transient') return false;

              const matches = element.contexts.filter((context) =>

                context.location.startsWith(
                  this.#getContextLocationParent(definitionContext.location)
                )
              );

              if (matches.length) return true;
              return false;
            }
          );

          if (representedOnes.length > 1)
            throw new Error(
              'Unhandled case of WITH materialization representation'
            );

          if (!representedOnes.length) return undefined;

          return {
            representativeName: ref.name,
            representativeAlias: ref.alias,
            representedName: representedOnes[0].name,
            representedSchemaName: representedOnes[0].schemaName,
            representedDatabaseName: representedOnes[0].databaseName,
            representedWarehouseName: representedOnes[0].warehouseName,
          };
        })
        .filter(isTransientMatRepresentation);

    return representations;
  };

  /* Assigns aliases for $ notation and columns with aliases */
  static #assignAliases = (
    statementRefsPrototype: RefsPrototype,
    catalog: CatalogModelData[],
    ): RefsPrototype => {

    statementRefsPrototype.columns.forEach((column, index) => {
      const thisCol = column;
      const nextCol = statementRefsPrototype.columns[index + 1];

      if (column.name.includes('$')) {
        
        const columnNumber = column.name.split('$')[1];
        const materializationNames =
          statementRefsPrototype.materializations.map((mat) => mat.name);

        const materializationName = 
        materializationNames.length === 1
          ? materializationNames[0]
          : column.materializationName || '';
        

        const filteredCatalog = catalog.filter(
          (model) =>
            materializationName &&
            model.materializationName.toUpperCase() === materializationName.toUpperCase()
            );

        const [realName] = filteredCatalog.map(
          (model) => model.columnNames[parseInt(columnNumber, 10) - 1]
        );

        thisCol.dependencyType = DependencyType.DATA;
        if (realName) thisCol.alias = realName;
      }

      if (!nextCol) return;
      if (
        column.dependencyType === DependencyType.DEFINITION &&
        nextCol.dependencyType === DependencyType.DATA
      )
        nextCol.alias = column.name;
    });

    return statementRefsPrototype;
  };

  /* Compares two ColumnRef objects if they are equal. Names can differ */
  static #whenColumnRefIsEqual = (fst: ColumnRef, snd: ColumnRef | undefined): boolean => {
    if (!fst || !snd) return false;

    return (
      fst.alias === snd.alias &&
      fst.databaseName === snd.databaseName &&
      fst.dependencyType === snd.dependencyType &&
      fst.isWildcardRef === snd.isWildcardRef &&
      fst.materializationName === snd.materializationName &&
      fst.context.path === snd.context.path &&
      fst.schemaName === snd.schemaName &&
      fst.warehouseName === snd.warehouseName
    );
  };

  /* Differentiates between the query and data dependency generated by a when clause */
  static #assignWhenClauseDependencies = (columns: ColumnRef[]): ColumnRef[] => {
    
    columns.forEach((col, elementIndex) => {
      const thisCol = col;
      const nextCol = columns[elementIndex+1];
      const isWhenClause = thisCol.context.path.includes(`${SQLElement.CASE_EXPRESSION}.${SQLElement.WHEN_CLAUSE}`);

      if(this.#whenColumnRefIsEqual(thisCol, nextCol) && isWhenClause)
        thisCol.dependencyType = DependencyType.QUERY;
    });

    return columns;
  };

  /* Transforms RefsPrototype object to Refs object by identifying missing materialization refs */
  static #buildStatementRefs = (
    refsPrototype: RefsPrototype,
    modelName: string,
    catalog: CatalogModelData[]
  ): Refs => {

    const aliasedRefsPrototype = this.#assignAliases(
      refsPrototype,
      catalog
    );

    const selfMaterializationRef = this.#buildSelfMaterializationRef(
      refsPrototype.materializations,
      modelName
    );

    const nonSelfMatRefsPrototypes = aliasedRefsPrototype.materializations.filter(
      (materialization) => {
        const contextLocations = materialization.contexts.map(
          (context) => context.location
        );
        const selfMatRefLocations = selfMaterializationRef.contexts.map(
          (context) => context.location
        );
        return contextLocations.every(
          (location) => !selfMatRefLocations.includes(location)
        );
      }
    );

    const nonSelfMaterializationRefs = this.#buildNonSelfMaterializationRefs(
      nonSelfMatRefsPrototypes
    );

    const materializations = nonSelfMaterializationRefs.concat(
      selfMaterializationRef
    );

    let columns = this.#buildColumnRefs(
      aliasedRefsPrototype.columns,
      nonSelfMaterializationRefs,
      selfMaterializationRef,
      catalog
    );

    const wildcards = this.#buildColumnRefs(
      aliasedRefsPrototype.wildcards,
      nonSelfMaterializationRefs,
      selfMaterializationRef,
      catalog
    );

    columns = this.#assignWhenClauseDependencies(columns);

    return { materializations, columns, wildcards };
  };

  /* Runs through tree of parsed logic and extract all refs of materializations and columns (self and parent materializations and columns) */
  static #getStatementRefs = (
    fileObj: any,
    modelName: string,
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

    return this.#buildStatementRefs(statementRefsPrototype, modelName, catalog);
  };

  static create = (prototype: LogicPrototype): Logic => {
    if (!prototype.id) throw new TypeError('Logic prototype must have id');
    if (!prototype.dbtModelId)
      throw new TypeError('Logic prototype must have dbtModelId');
    if (!prototype.modelName)
      throw new TypeError('Logic prototype must have model name');
    if (!prototype.sql)
      throw new TypeError('Logic prototype must have SQL logic');
    if (!prototype.parsedLogic)
      throw new TypeError('Logic  prototype must have parsed SQL logic');
    if (!prototype.lineageId) throw new TypeError('Logic must have lineageId');
    if (!prototype.catalog)
      throw new TypeError('Logic prototype must have catalog data');

    const parsedLogicObj = JSON.parse(prototype.parsedLogic);

    const statementRefs = this.#getStatementRefs(
      parsedLogicObj.file,
      prototype.modelName,
      prototype.catalog
    );

    const logic = this.build({
      id: prototype.id,
      dbtModelId: prototype.dbtModelId,
      sql: prototype.sql,
      dependentOn: prototype.dependentOn,
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
      dependentOn: properties.dependentOn,
      parsedLogic: properties.parsedLogic,
      statementRefs: properties.statementRefs,
      lineageId: properties.lineageId,
    });

    return logic;
  };
}
