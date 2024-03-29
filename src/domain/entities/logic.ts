import { DependencyType } from './dependency';
import SQLElement from '../value-types/sql-element';

export interface MaterializationDefinition {
  relationName: string;
  materializationName: string;
  schemaName?: string;
  databaseName?: string;
}

export interface DependentOn {
  dbtDependencyDefinitions: MaterializationDefinition[];
  dwDependencyDefinitions: MaterializationDefinition[];
}

interface DbtPrototypeProps {
  dbtDependentOn: MaterializationDefinition[];
}

interface GeneralPrototypeProps {
  id: string;
  relationName: string;
  sql: string;
  parsedLogic: string;
  catalog: ModelRepresentation[];
}

export interface LogicPrototype {
  generalProps: GeneralPrototypeProps;
  dbtProps?: DbtPrototypeProps;
}
export interface LogicProps {
  id: string;
  relationName: string;
  sql: string;
  dependentOn: DependentOn;
  parsedLogic: string;
  statementRefs: Refs;
}

type LogicDto = LogicProps;

export interface ModelRepresentation extends MaterializationDefinition {
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

type AmbiguityType = 'potential-compound-val-ref';

interface ColumnRefPrototype extends Ref {
  dependencyType: DependencyType;
  isWildcardRef: boolean;
  // todo - to be replaced by generalized isTransient type
  isCompoundValueRef: boolean;
  ambiguityType?: AmbiguityType;
  materializationName?: string;
  context: RefContext;
  usesSelfMaterialization?: boolean;
}

export interface ColumnRef
  extends Omit<ColumnRefPrototype, 'materializationName' | 'ambiguityType'> {
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
  tempData: TempExtractionData;
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
  key: string;
  alias?: Alias;
  value: any;
  refPath: string;
  refsPrototype: RefsPrototype;
  contextLocation: string;
  recursionLevel: number;
  path?: string;
}

interface HandlerReturn {
  newAlias?: Alias;
  newPrototype: RefsPrototype;
}

export class Logic {
  #id: string;

  #relationName: string;

  #sql: string;

  #dependentOn: DependentOn;

  #parsedLogic: string;

  #statementRefs: Refs;

  get id(): string {
    return this.#id;
  }

  get relationName(): string {
    return this.#relationName;
  }

  get sql(): string {
    return this.#sql;
  }

  get dependentOn(): DependentOn {
    return this.#dependentOn;
  }

  get parsedLogic(): string {
    return this.#parsedLogic;
  }

  get statementRefs(): Refs {
    return this.#statementRefs;
  }

  private constructor(properties: LogicProps) {
    this.#id = properties.id;
    this.#relationName = properties.relationName;
    this.#sql = properties.sql;
    this.#dependentOn = properties.dependentOn;
    this.#parsedLogic = properties.parsedLogic;
    this.#statementRefs = properties.statementRefs;
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
      return 'definition';
    if (dataDependencyElementsRegex.some((element) => element.test(path)))
      return 'data';
    return 'query';
  };

  /* Returns a single joined string value of an array value identified in the parsed SQL logic
  (e.g. for for table_expressions or column_expressions) */
  static #joinArrayValue = (value: [{ [key: string]: string }]): string => {
    const valueElements = value.map((element) => {
      const dotKey = 'dot';
      if (SQLElement.IDENTIFIER in element)
        return element[SQLElement.IDENTIFIER];
      if (SQLElement.IDENTIFIER_NAKED in element)
        return element[SQLElement.IDENTIFIER_NAKED];
      if (SQLElement.IDENTIFIER_QUOTED in element)
        return element[SQLElement.IDENTIFIER_QUOTED].replace('"', '');
      if (dotKey in element) return element[dotKey];

      throw new RangeError('Unhandled value chaining error');
    });

    return valueElements.join('');
  };

  /* Checks if column ref represents a ref to an external compound value */
  static #isCompoundValueRef = (path: string): boolean => {
    const compoundValueIndicators: RegExp[] = [
      new RegExp(
        `${SQLElement.FROM_EXPRESSION_ELEMENT}.*${SQLElement.COLUMN_REFERENCE}.${SQLElement.IDENTIFIER}`
      ),
    ];

    return compoundValueIndicators.some((element) => element.test(path));
  };

  /* Checks for specific column references e.g. compound value references (<smthng>.value)
  which can not be clearly identified at this stage. Can be extended by adding other
  ambiguous use cases */
  static #getPotentialAmbiguityType = (
    columnName: string
  ): AmbiguityType | undefined => {
    if (columnName === 'value') return 'potential-compound-val-ref';
    return undefined;
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

    const ambiguityType = this.#getPotentialAmbiguityType(
      columnValueRef.columnName
    );

    return {
      dependencyType: this.#getColumnDependencyType(path),
      alias: props.alias,
      name: columnValueRef.columnName,
      materializationName: columnValueRef.materializationName,
      schemaName: columnValueRef.schemaName,
      databaseName: columnValueRef.databaseName,
      warehouseName: columnValueRef.warehouseName,
      isWildcardRef: false,
      isCompoundValueRef: this.#isCompoundValueRef(path),
      ambiguityType: ambiguityType || undefined,
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

    const identifiers = [
      SQLElement.WILDCARD_IDENTIFIER_IDENTIFIER,
      SQLElement.WILDCARD_IDENTIFIER_NAKED,
      SQLElement.WILDCARD_IDENTIFIER_QUOTED,
    ]
      .map((key) => {
        if (wildcardElementKeys.includes(key)) return key;
        return undefined;
      })
      .filter((el): el is string => !!el);

    if (identifiers.length > 1)
      throw new SyntaxError(
        'Unhandled wildcard syntax - multiple identifier found'
      );

    const isDotNoation =
      identifiers.length &&
      [
        SQLElement.WILDCARD_IDENTIFIER_DOT,
        SQLElement.WILDCARD_IDENTIFIER_STAR,
      ].every((wildcardElementKey) =>
        wildcardElementKeys.includes(wildcardElementKey)
      );

    const path = this.#appendPath(props.key, props.path);

    if (isDotNoation) {
      const identifier = identifiers[0];
      const identifierValue = props.value[identifier];

      if (typeof identifierValue !== 'string')
        throw new SyntaxError(
          `Unhandled wildcard identifier value type: ${typeof identifierValue} `
        );

      return {
        dependencyType: this.#getColumnDependencyType(path),
        name: SQLElement.WILDCARD_IDENTIFIER_STAR,
        materializationName:
          identifier === SQLElement.WILDCARD_IDENTIFIER_QUOTED
            ? identifierValue.replace('"', '')
            : identifierValue,
        isWildcardRef: true,
        isCompoundValueRef: this.#isCompoundValueRef(path),
        ambiguityType: undefined,
        context: { path, location: props.contextLocation },
      };
    }
    if (
      wildcardElementKeys.length === 1 &&
      wildcardElementKeys.includes(SQLElement.WILDCARD_IDENTIFIER_STAR)
    )
      return {
        dependencyType: this.#getColumnDependencyType(path),
        name: SQLElement.WILDCARD_IDENTIFIER_STAR,
        isWildcardRef: true,
        isCompoundValueRef: this.#isCompoundValueRef(path),
        ambiguityType: undefined,
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
        tempData: { alias: subExtractionDto.tempData.alias },
      };
    }

    if (subExtractionDto.tempData.alias)
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
      tempData: { alias: localAlias },
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
  static #handleIdentifiers = (input: ExtractRefProperties): HandlerReturn => {
    if (!input.path) throw new ReferenceError('Path should not be undefined');

    const newPrototype = input.refsPrototype;
    let newAlias = input.alias;

    if (input.path.includes(SQLElement.ALIAS_EXPRESSION)) {
      newAlias = {
        key: input.key,
        value: input.value,
        refPath: input.refPath,
        boundedContext: this.#getContextLocationParent(
          this.#getContextLocationParent(input.contextLocation)
        ),
        isUsed: false,
      };
    } else if (input.path.includes(SQLElement.COLUMN_REFERENCE)) {
      newPrototype.columns.push(
        this.#handleColumnRef({
          key: input.key,
          value: input.value,
          path: input.refPath,
          alias: input.alias ? input.alias.value : undefined,
          contextLocation: input.contextLocation,
        })
      );

      if (newAlias) newAlias.isUsed = true;
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

      if (newAlias) newAlias.isUsed = true;
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
    elementIndex: number
  ): HandlerReturn => {
    if (input.recursionLevel === undefined)
      throw new ReferenceError('Recursion level should not be undefined');

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

    newAlias = mergeExtractionDto.tempData.alias;
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
      { refsPrototype: withElementsMerged, tempData: {} },
      input.alias
    );
    newPrototype = mergeExtractionDto.refsPrototype;
    newAlias = mergeExtractionDto.tempData.alias;

    return { newAlias, newPrototype };
  };

  /* In case an alias was not initially allocated and is about to expire
   this function tries to find its targets within given context */
  static #allocateUnusedAlias = (
    alias: Alias,
    refsPrototype: RefsPrototype
  ): RefsPrototype => {
    const aliasToAllocate = alias;

    const relevantColumns = refsPrototype.columns.filter(
      (column) => !column.alias
    );
    const relevantMaterializations = refsPrototype.materializations.filter(
      (materialization) => !materialization.alias
    );
    if (!relevantColumns.length && relevantMaterializations.length)
      throw new RangeError('Unmatched alias - No target found');

    const columnSubContexts = relevantColumns.map((column) => {
      const { location } = column.context;

      return location.slice(
        aliasToAllocate.boundedContext.length + 1,
        aliasToAllocate.boundedContext.length + 2
      );
    });

    const numberUniqueColContexts: number = new Set(columnSubContexts).size;

    if (numberUniqueColContexts > 1)
      throw new RangeError(
        'Unmatched alias - more than one potential column context identified'
      );

    const materializationSubContexts = relevantMaterializations
      .map((materialization) => {
        const locations = materialization.contexts.map(
          (context) => context.location
        );

        const subContexts = locations.map((location) =>
          location.slice(
            aliasToAllocate.boundedContext.length + 1,
            aliasToAllocate.boundedContext.length + 2
          )
        );
        return subContexts;
      })
      .flat();

    const numberUniqueMatContexts: number = new Set(materializationSubContexts)
      .size;

    if (numberUniqueMatContexts > 1)
      throw new RangeError(
        'Unmatched alias - more than one potential materialization context identified'
      );

    if (numberUniqueColContexts && numberUniqueMatContexts)
      throw new RangeError(
        'Unmatched alias - matching columns and materializations found'
      );

    if (numberUniqueColContexts) {
      const updatedColumns = refsPrototype.columns.map((column) => {
        const columnToUpdate = column;
        columnToUpdate.alias = aliasToAllocate.value;
        return columnToUpdate;
      });

      const updatedRefsPrototype: RefsPrototype = {
        materializations: refsPrototype.materializations,
        wildcards: refsPrototype.wildcards,
        columns: updatedColumns,
      };

      return updatedRefsPrototype;
    }

    const updatedMaterializations = refsPrototype.materializations.map(
      (materialization) => {
        const materializationToUpdate = materialization;
        materializationToUpdate.alias = aliasToAllocate.value;
        return materializationToUpdate;
      }
    );

    const updatedRefsPrototype: RefsPrototype = {
      materializations: updatedMaterializations,
      wildcards: refsPrototype.wildcards,
      columns: refsPrototype.columns,
    };

    return updatedRefsPrototype;
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

        if (alias) alias.isUsed = true;
      } else if (value.constructor === Object) {
        const { newAlias, newPrototype } = this.#handleValueObject(
          {
            key,
            alias,
            value,
            refPath,
            refsPrototype,
            contextLocation,
            recursionLevel: recursionIndex,
          },
          elementIndex
        );

        alias = newAlias;
        refsPrototype = newPrototype;
      } else if (Object.prototype.toString.call(value) === '[object Array]') {
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

          if (alias) alias.isUsed = true;
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
        } else
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
            alias = mergeExtractionDto.tempData.alias;
          });
      }
    });

    if (!alias) return { tempData, refsPrototype };

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
    else if (aliasExpired && alias.isUsed) return { tempData, refsPrototype };
    else if (aliasExpired && !alias.isUsed) {
      const updatedRefsPrototype = this.#allocateUnusedAlias(
        alias,
        refsPrototype
      );
      alias.isUsed = true;
      return { tempData, refsPrototype: updatedRefsPrototype };
    } else if (!aliasExhausted) tempData.alias = alias;

    return { tempData, refsPrototype };
  };

  /* Identifies the closest materialization reference to a provided column path. 
  Assumption: The closest materialization ref in SQL defines the the ref's materialization */
  static #getBestMatchingMaterialization = (
    columnPath: string,
    materializations: MaterializationRef[],
    columnName: string,
    catalog: ModelRepresentation[]
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
    relationName: string
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
      return { name: relationName, type: 'self', contexts: [] };

    return { ...selfMatPrototype, name: relationName, type: 'self' };
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

    const schemaNameIsEqual =
      !prototype.schemaName ||
      prototype.schemaName === selfMaterializationRef.schemaName;

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

  static #isResourceFromSelfMaterialization = (
    columnName: string,
    columnRefPrototypes: ColumnRefPrototype[]
  ): boolean => {
    const areAliasesUsedAsName = columnRefPrototypes.map(
      (prototype) => prototype.alias === columnName
    );
    return areAliasesUsedAsName.includes(true);
  };

  /* Transforming prototypes to columnRefs, by improving information coverage on object level  */
  static #buildColumnRefs = (
    columnRefPrototypes: ColumnRefPrototype[],
    nonSelfMaterializationRefs: MaterializationRef[],
    selfMaterializationRef: MaterializationRef,
    catalog: ModelRepresentation[]
  ): ColumnRef[] => {
    columnRefPrototypes.forEach((prototype) => {
      const thisPrototype = prototype;
      thisPrototype.usesSelfMaterialization =
        this.#isResourceFromSelfMaterialization(
          prototype.name,
          columnRefPrototypes
        );

      if (!thisPrototype.isWildcardRef || !thisPrototype.materializationName)
        return;

      nonSelfMaterializationRefs.forEach((matRef) => {
        if (thisPrototype.materializationName === matRef.alias)
          thisPrototype.materializationName = matRef.name;
      });
    });

    const columns: ColumnRef[] = columnRefPrototypes.map((prototype) => {
      const transientMatRepresentations = this.#getTransientRepresentations(
        nonSelfMaterializationRefs
      );

      if (prototype.dependencyType === 'definition')
        return {
          alias: prototype.alias,
          context: prototype.context,
          dependencyType: prototype.dependencyType,
          isCompoundValueRef: prototype.isCompoundValueRef,
          isWildcardRef: prototype.isWildcardRef,
          name: prototype.name,
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
          alias: prototype.alias,
          context: prototype.context,
          dependencyType: prototype.dependencyType,
          isCompoundValueRef: prototype.isCompoundValueRef,
          isWildcardRef: prototype.isWildcardRef,
          name: prototype.name,
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

        if (isSelfRefColumn) columnRef.dependencyType = 'definition';

        return columnRef;
      }

      const materializations = nonSelfMaterializationRefs.concat(
        selfMaterializationRef
      );

      let materializationRef: MaterializationRef;

      if (prototype.usesSelfMaterialization) {
        const selfMaterialization = materializations.filter(
          (materialization) => materialization.type === 'self'
        );
        if (selfMaterialization.length !== 1)
          throw new ReferenceError(
            'Multiple or no self materialisations exist'
          );
        [materializationRef] = selfMaterialization;
      } else {
        materializationRef = this.#getBestMatchingMaterialization(
          prototype.context.path,
          materializations,
          prototype.name,
          catalog
        );
      }

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
        columnRef.dependencyType = 'definition';

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
    catalog: ModelRepresentation[]
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
            model.materializationName.toUpperCase() ===
              materializationName.toUpperCase()
        );

        const [realName] = filteredCatalog.map(
          (model) => model.columnNames[parseInt(columnNumber, 10) - 1]
        );

        thisCol.dependencyType = 'data';
        if (realName) thisCol.alias = realName;
      }

      if (!nextCol) return;
      if (
        column.dependencyType === 'definition' &&
        nextCol.dependencyType === 'data'
      )
        nextCol.alias = column.name;
    });

    return statementRefsPrototype;
  };

  /* Compares two ColumnRef objects if they are equal. Names and dependency type can differ */
  static #whenClauseColumnRefProtoAreEqual = (
    fst: ColumnRefPrototype | undefined,
    snd: ColumnRefPrototype | undefined
  ): boolean => {
    if (!fst || !snd) return false;

    return (
      fst.alias === snd.alias &&
      fst.materializationName === snd.materializationName &&
      fst.schemaName === snd.schemaName &&
      fst.databaseName === snd.databaseName &&
      fst.warehouseName === snd.warehouseName &&
      fst.isWildcardRef === snd.isWildcardRef &&
      fst.context.path === snd.context.path
    );
  };

  /* Differentiates between the query and data dependency generated by a when clause */
  static #assignWhenClauseDependencies = (
    columnPrototypes: ColumnRefPrototype[]
  ): ColumnRefPrototype[] => {
    columnPrototypes.forEach((colPrototype, elementIndex) => {
      const prevPrototype = columnPrototypes[elementIndex - 1];
      const thisPrototype = colPrototype;
      const nextPrototype = columnPrototypes[elementIndex + 1];

      const isWhenClause = thisPrototype.context.path.includes(
        `${SQLElement.CASE_EXPRESSION}.${SQLElement.WHEN_CLAUSE}`
      );

      const singleWhenClause =
        isWhenClause &&
        !this.#whenClauseColumnRefProtoAreEqual(prevPrototype, thisPrototype) &&
        !this.#whenClauseColumnRefProtoAreEqual(thisPrototype, nextPrototype);

      const firstInWhenSequence =
        isWhenClause &&
        this.#whenClauseColumnRefProtoAreEqual(thisPrototype, nextPrototype) &&
        !(prevPrototype.dependencyType === 'query');

      if (firstInWhenSequence || singleWhenClause)
        thisPrototype.dependencyType = 'query';
    });

    return columnPrototypes;
  };

  /* Compares 2 column refs and determines if they represent the same dependency. Context need not
   be equal as their origins may differ */
  static #createdDependenciesAreEqual = (
    testCol: ColumnRefPrototype,
    col: ColumnRefPrototype
  ): boolean =>
    testCol.dependencyType === col.dependencyType &&
    testCol.alias === col.alias &&
    testCol.name === col.name &&
    testCol.materializationName === col.materializationName &&
    testCol.schemaName === col.schemaName &&
    testCol.databaseName === col.databaseName &&
    testCol.warehouseName === col.warehouseName &&
    testCol.isWildcardRef === col.isWildcardRef;

  /* Removes any dependencies that have been created from both the then and else branches */
  static #removePossibleDuplicateDependencies = (
    columnPrototypes: ColumnRefPrototype[]
  ): ColumnRefPrototype[] => {
    const uniqueDependencies = columnPrototypes.filter(
      (colPrototype, elementIndex, self) =>
        elementIndex ===
        self.findIndex((testColPrototype) =>
          this.#createdDependenciesAreEqual(testColPrototype, colPrototype)
        )
    );

    return uniqueDependencies;
  };

  /* Clears ambiguity of column references */
  static #clearAmbiguity = (refsPrototype: RefsPrototype): RefsPrototype => {
    const explicitColumnRefs = refsPrototype.columns.map(
      (column: ColumnRefPrototype) => {
        if (!column.ambiguityType) return column;

        if (column.ambiguityType === 'potential-compound-val-ref') {
          const compoundValueRefs = refsPrototype.columns.filter(
            (element) => element.isCompoundValueRef
          );
          if (!compoundValueRefs.length) return column;

          const columnMatName = column.materializationName
            ? column.materializationName
            : undefined;

          const matchingCompoundValueRefs = compoundValueRefs.filter((ref) => {
            const refAlias = ref.alias ? ref.alias : undefined;
            const refName = ref.name ? ref.name : undefined;

            return columnMatName === refAlias || columnMatName === refName;
          });

          if (!matchingCompoundValueRefs.length) return column;
          if (matchingCompoundValueRefs.length > 1)
            throw new RangeError('Multiple matching compound value refs found');

          return {
            ...column,
            ambiguityType: undefined,
            name: matchingCompoundValueRefs[0].name,
            materializationName:
              matchingCompoundValueRefs[0].materializationName,
            schemaName: matchingCompoundValueRefs[0].schemaName,
            databaseName: matchingCompoundValueRefs[0].databaseName,
            warehouseName: matchingCompoundValueRefs[0].warehouseName,
          };
        }

        throw new RangeError('Unhandled column ambiguity type');
      }
    );

    const prototypeToClear = refsPrototype;

    prototypeToClear.columns = explicitColumnRefs;

    return prototypeToClear;
  };

  /* Transforms RefsPrototype object to Refs object by identifying missing materialization refs */
  static #buildStatementRefs = (
    refsPrototype: RefsPrototype,
    relationName: string,
    catalog: ModelRepresentation[]
  ): Refs => {
    const explicitRefsPrototype = this.#clearAmbiguity(refsPrototype);

    const aliasedRefsPrototype = this.#assignAliases(
      explicitRefsPrototype,
      catalog
    );

    const selfMaterializationRef = this.#buildSelfMaterializationRef(
      refsPrototype.materializations,
      relationName
    );

    const nonSelfMatRefsPrototypes =
      aliasedRefsPrototype.materializations.filter((materialization) => {
        const contextLocations = materialization.contexts.map(
          (context) => context.location
        );
        const selfMatRefLocations = selfMaterializationRef.contexts.map(
          (context) => context.location
        );
        return contextLocations.every(
          (location) => !selfMatRefLocations.includes(location)
        );
      });

    const nonSelfMaterializationRefs = this.#buildNonSelfMaterializationRefs(
      nonSelfMatRefsPrototypes
    );

    const materializations = nonSelfMaterializationRefs.concat(
      selfMaterializationRef
    );

    aliasedRefsPrototype.columns = this.#assignWhenClauseDependencies(
      aliasedRefsPrototype.columns
    );
    aliasedRefsPrototype.columns = this.#removePossibleDuplicateDependencies(
      aliasedRefsPrototype.columns
    );

    const columns = this.#buildColumnRefs(
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

    return { materializations, columns, wildcards };
  };

  /* Runs through tree of parsed logic and extract all refs of materializations and columns (self and parent materializations and columns) */
  static #getStatementRefs = (
    fileObj: any,
    relationName: string,
    catalog: ModelRepresentation[]
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

    return this.#buildStatementRefs(
      statementRefsPrototype,
      relationName,
      catalog
    );
  };

  /* Retrieving relation names for external non-dbt mats that are referenced by sql model logic */
  static #getDwDependencyDefinitions = (
    materializationRefs: MaterializationRef[],
    dbtDependencyDefinitions: MaterializationDefinition[]
  ): MaterializationDefinition[] => {
    const isDefinition = (
      definition: MaterializationDefinition | undefined
    ): definition is MaterializationDefinition => !!definition;

    const mappingResults = materializationRefs
      .filter((ref) => ref.type === 'dependency')
      .map((ref: MaterializationRef): MaterializationDefinition | undefined => {
        if (!ref.databaseName) {
          console.warn(
            `Mat (name: ${ref.name}, alias: ${ref.alias}, schemaName: ${ref.schemaName}) is missing databaseName`
          );

          return undefined;
        }

        if (!ref.schemaName) {
          console.warn(
            `Mat (name: ${ref.name}, alias: ${ref.alias}, databaseName: ${ref.databaseName}) is missing schemaName`
          );
          return undefined;
        }

        const potentiallyMissingRelationName = `${ref.databaseName}.${ref.schemaName}.${ref.name}`;

        const matchingDefinitions = dbtDependencyDefinitions.filter(
          (el) =>
            el.relationName.replace(/"/g, '') === potentiallyMissingRelationName
        );

        if (matchingDefinitions.length) return undefined;

        return {
          relationName: potentiallyMissingRelationName,
          materializationName: ref.name,
          schemaName: ref.schemaName,
          databaseName: ref.databaseName,
        };
      })
      .filter(isDefinition);

    return mappingResults;
  };

  static create = (prototype: LogicPrototype): Logic => {
    const { generalProps, dbtProps } = prototype;

    if (!generalProps.id) throw new TypeError('Logic prototype must have id');
    if (!generalProps.relationName)
      throw new TypeError('Logic prototype must have relationName');
    if (!generalProps.sql)
      throw new TypeError('Logic prototype must have SQL logic');
    if (!generalProps.parsedLogic)
      throw new TypeError('Logic  prototype must have parsed SQL logic');
    if (!generalProps.catalog)
      throw new TypeError('Logic prototype must have catalog data');

    const parsedLogicObj = JSON.parse(generalProps.parsedLogic);

    const statementRefs = this.#getStatementRefs(
      parsedLogicObj.file,
      generalProps.relationName,
      generalProps.catalog
    );

    const dwDependencyDefinitions = this.#getDwDependencyDefinitions(
      statementRefs.materializations,
      dbtProps ? dbtProps.dbtDependentOn : []
    );

    const dependentOn: DependentOn = {
      dbtDependencyDefinitions: dbtProps ? dbtProps.dbtDependentOn : [],
      dwDependencyDefinitions,
    };

    const logic = this.build({
      id: generalProps.id,
      relationName: generalProps.relationName,
      sql: generalProps.sql,
      dependentOn,
      parsedLogic: generalProps.parsedLogic,
      statementRefs,
    });

    return logic;
  };

  static build = (properties: LogicProps): Logic => {
    if (!properties.id) throw new TypeError('Logic must have id');
    if (!properties.relationName)
      throw new TypeError('Logic must have relationName');
    if (!properties.parsedLogic)
      throw new TypeError('Logic creation requires parsed SQL logic');

    const logic = new Logic(properties);

    return logic;
  };

  toDto = (): LogicDto => ({
    id: this.#id,
    relationName: this.#relationName,
    sql: this.#sql,
    dependentOn: this.#dependentOn,
    parsedLogic: this.#parsedLogic,
    statementRefs: this.#statementRefs,
  });
}
