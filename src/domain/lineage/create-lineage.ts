// todo - Clean Architecture dependency violation. Fix
import fs from 'fs';
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import SQLElement from '../value-types/sql-element';
import { CreateColumn } from '../column/create-column';
import { CreateMaterialization } from '../materialization/create-materialization';
import { buildLineageDto, LineageDto } from './lineage-dto';
import { CreateLogic } from '../logic/create-logic';
import { ParseSQL, ParseSQLResponseDto } from '../sql-parser-api/parse-sql';
import { Lineage } from '../entities/lineage';
import LineageRepo from '../../infrastructure/persistence/lineage-repo';
import { Logic, ColumnRef, Refs } from '../entities/logic';
import { CreateDependency } from '../dependency/create-dependency';
import { DependencyType } from '../entities/dependency';
import { ReadColumns } from '../column/read-columns';

export interface CreateLineageRequestDto {
  lineageId?: string;
  lineageCreatedAt?: number;
}

export interface CreateLineageAuthDto {
  organizationId: string;
}

export type CreateLineageResponseDto = Result<LineageDto>;

export class CreateLineage
  implements
    IUseCase<
      CreateLineageRequestDto,
      CreateLineageResponseDto,
      CreateLineageAuthDto
    >
{
  readonly #createLogic: CreateLogic;

  readonly #createMaterialization: CreateMaterialization;

  readonly #createColumn: CreateColumn;

  readonly #createDependency: CreateDependency;

  readonly #parseSQL: ParseSQL;

  readonly #lineageRepo: LineageRepo;

  readonly #readColumns: ReadColumns;

  #lineage?: Lineage;

  #logics: Logic[];

  #lastQueryDependency?: ColumnRef;

  constructor(
    createLogic: CreateLogic,
    createMaterialization: CreateMaterialization,
    createColumn: CreateColumn,
    createDependency: CreateDependency,
    parseSQL: ParseSQL,
    lineageRepo: LineageRepo,
    readColumns: ReadColumns
  ) {
    this.#createLogic = createLogic;
    this.#createMaterialization = createMaterialization;
    this.#createColumn = createColumn;
    this.#createDependency = createDependency;
    this.#parseSQL = parseSQL;
    this.#lineageRepo = lineageRepo;
    this.#readColumns = readColumns;
    this.#logics = [];
    this.#lineage = undefined;
    this.#lastQueryDependency = undefined;
  }

  /* Building a new lineage object that is referenced by resources like columns and materializations */
  #buildLineage = async (
    lineageId?: string,
    lineageCreatedAt?: number
  ): Promise<void> => {
    // todo - enable lineage updating
    // this.#lineage =
    //   lineageId && lineageCreatedAt
    //     ? Lineage.create({
    //         id: lineageId,
    //         createdAt: lineageCreatedAt,
    //       })
    //     : Lineage.create({ id: new ObjectId().toHexString() });

    this.#lineage = Lineage.create({ id: new ObjectId().toHexString() });

    if (!(lineageId && lineageCreatedAt))
      await this.#lineageRepo.insertOne(this.#lineage);
  };

  /* Parses SQL logic */
  #parseLogic = async (sql: string): Promise<string> => {
    const parseSQLResult: ParseSQLResponseDto = await this.#parseSQL.execute(
      { dialect: 'snowflake', sql },
      { jwt: 'todo' }
    );

    if (!parseSQLResult.success) throw new Error(parseSQLResult.error);
    if (!parseSQLResult.value)
      throw new SyntaxError(`Parsing of SQL logic failed`);

    return JSON.stringify(parseSQLResult.value);
  };

  /* Get dbt nodes from catalog.json or manifest.json */
  #getDbtResources = (location: string): any => {
    const data = fs.readFileSync(location, 'utf-8');

    const catalog = JSON.parse(data);

    const { nodes } = catalog;
    const { sources } = catalog;

    return { ...nodes, ...sources };
  };

  /* Runs through dbt nodes and creates objects like logic, materializations and columns */
  #generateWarehouseResources = async (): Promise<void> => {
    const dbtCatalogResources = this.#getDbtResources(
      `C:/Users/felix-pc/Documents/Repositories/lineage-analysis/test/use-cases/dbt/catalog/web-samples/sample-1.json`
      // `C:/Users/nasir/OneDrive/Desktop/lineage-analysis/test/use-cases/dbt/catalog/web-samples/sample-1.json`
    );
    const dbtManifestResources = this.#getDbtResources(
      `C:/Users/felix-pc/Documents/Repositories/lineage-analysis/test/use-cases/dbt/manifest/web-samples/sample-1.json`
      // `C:/Users/nasir/OneDrive/Desktop/lineage-analysis/test/use-cases/dbt/manifest/web-samples/sample-1.json`
    );

    const dbtModelKeys = Object.keys(dbtCatalogResources);

    if (!dbtModelKeys.length) throw new ReferenceError('No dbt models found');

    await Promise.all(
      dbtModelKeys.map(async (key) => {
        if (!this.#lineage)
          throw new ReferenceError('Lineage property is undefined');
        const lineage = this.#lineage;

        const dbtModel = dbtCatalogResources[key];
        const manifest = dbtManifestResources[key];

        const parsedLogic = await this.#parseLogic(manifest.compiled_sql);

        const createLogicResult = await this.#createLogic.execute(
          {
            dbtModelId: dbtModel.unique_id,
            lineageId: lineage.id,
            parsedLogic,
          },
          { organizationId: 'todo' }
        );

        if (!createLogicResult.success)
          throw new Error(createLogicResult.error);
        if (!createLogicResult.value)
          throw new SyntaxError(`Creation of logic failed`);

        const logic = createLogicResult.value;

        this.#logics.push(logic);

        const createMaterializationResult =
          await this.#createMaterialization.execute(
            {
              materializationType: dbtModel.metadata.type,
              name: dbtModel.metadata.name,
              dbtModelId: dbtModel.unique_id,
              schemaName: dbtModel.metadata.schema,
              databaseName: dbtModel.metadata.database,
              logicId: logic.id,
              lineageId: lineage.id,
            },
            { organizationId: 'todo' }
          );

        if (!createMaterializationResult.success)
          throw new Error(createMaterializationResult.error);
        if (!createMaterializationResult.value)
          throw new SyntaxError(`Creation of materialization failed`);

        const materialization = createMaterializationResult.value;

        await Promise.all(
          Object.keys(dbtModel.columns).map(async (columnKey) => {
            if (!lineage)
              throw new ReferenceError('Lineage property is undefined');

            const column = dbtModel.columns[columnKey];

            // todo - add additional properties like index
            const createColumnResult = await this.#createColumn.execute(
              {
                dbtModelId: materialization.dbtModelId,
                name: column.name,
                index: column.index,
                type: column.type,
                materializationId: materialization.id,
                lineageId: lineage.id,
              },
              { organizationId: 'todo' }
            );

            if (!createColumnResult.success)
              throw new Error(createColumnResult.error);
            if (!createColumnResult.value)
              throw new SyntaxError(`Creation of column failed`);
          })
        );
      })
    );
  };

  /* Identifies the statement root (e.g. create_materialization_statement.select_statement) of a specific reference path */
  #getStatementRoot = (path: string): string => {
    const lastIndexStatementRoot = path.lastIndexOf(SQLElement.STATEMENT);
    if (lastIndexStatementRoot === -1 || !lastIndexStatementRoot)
      // todo - inconsistent usage of Error types. Sometimes Range and sometimes Reference
      throw new RangeError('Statement root not found for column reference');

    return path.slice(0, lastIndexStatementRoot + SQLElement.STATEMENT.length);
  };

  /* Checks if parent dependency can be mapped on the provided self column or to another column of the self materialization. */
  #isDependencyOfTarget = (
    potentialDependency: ColumnRef,
    selfRef: ColumnRef
  ): boolean => {
    const dependencyStatementRoot = this.#getStatementRoot(
      potentialDependency.path
    );
    const selfStatementRoot = this.#getStatementRoot(selfRef.path);

    const isStatementDependency =
      !potentialDependency.path.includes(SQLElement.INSERT_STATEMENT) &&
      !potentialDependency.path.includes(SQLElement.COLUMN_DEFINITION) &&
      dependencyStatementRoot === selfStatementRoot &&
      (potentialDependency.path.includes(SQLElement.COLUMN_REFERENCE) ||
        potentialDependency.path.includes(SQLElement.WILDCARD_IDENTIFIER));

    if (!isStatementDependency) return false;

    const isSelfSelectStatement = selfStatementRoot.includes(
      SQLElement.SELECT_STATEMENT
    );

    const isWildcardRef =
      isSelfSelectStatement && potentialDependency.isWildcardRef;
    const isSameName =
      isSelfSelectStatement && selfRef.name === potentialDependency.name;
    const isGroupBy =
      potentialDependency.path.includes(SQLElement.GROUPBY_CLAUSE) &&
      selfRef.name !== potentialDependency.name;
    const isJoinOn =
      potentialDependency.path.includes(SQLElement.JOIN_ON_CONDITION) &&
      selfRef.name !== potentialDependency.name;

    if (isWildcardRef || isSameName || isGroupBy) return true;

    if (isJoinOn) return false;
    if (potentialDependency.name !== selfRef.name) return false;

    throw new RangeError(
      'Unhandled case when checking if is dependency of target'
    );
  };

  /* Get all relevant statement references that are a valid dependency to parent resources */
  #getDataDependencyRefs = (statementRefs: Refs): ColumnRef[] => {
    let dataDependencyRefs = statementRefs.columns.filter(
      (column) => column.dependencyType === DependencyType.DATA
    );
    dataDependencyRefs.push(...statementRefs.wildcards);

    const setColumnRefs = dataDependencyRefs.filter((ref) =>
      ref.path.includes(SQLElement.SET_EXPRESSION)
    );

    const uniqueSetColumnRefs = setColumnRefs.filter(
      (value, index, self) =>
        index ===
        self.findIndex(
          (ref) =>
            ref.name === value.name &&
            ref.path === value.path &&
            ref.materializationName === value.materializationName
        )
    );

    let columnRefs = dataDependencyRefs.filter(
      (ref) => !ref.path.includes(SQLElement.SET_EXPRESSION)
    );

    dataDependencyRefs = uniqueSetColumnRefs.concat(columnRefs);

    const withColumnRefs = dataDependencyRefs.filter(
      (ref) =>
        ref.path.includes(SQLElement.WITH_COMPOUND_STATEMENT) &&
        !ref.path.includes(SQLElement.COMMON_TABLE_EXPRESSION)
    );
    columnRefs = dataDependencyRefs.filter(
      (ref) => !ref.path.includes(SQLElement.WITH_COMPOUND_STATEMENT)
    );

    dataDependencyRefs = withColumnRefs.concat(columnRefs);

    return dataDependencyRefs;
  };

  #columnRefIsEqual = (fst: ColumnRef, snd: ColumnRef | undefined): boolean => {
    if (!fst || !snd) return false;

    return (
      fst.alias === snd.alias &&
      fst.databaseName === snd.databaseName &&
      fst.dependencyType === snd.dependencyType &&
      fst.isWildcardRef === snd.isWildcardRef &&
      fst.materializationName === snd.materializationName &&
      fst.name === snd.name &&
      fst.path === snd.path &&
      fst.schemaName === snd.schemaName &&
      fst.warehouseName === snd.warehouseName
    );
  };

  /* Creates all dependencies that exist between DWH resources */
  #buildDependency = async (
    selfRef: ColumnRef,
    statementRefs: Refs,
    dbtModelId: string
  ): Promise<void> => {
    const isColumnRef = (item: ColumnRef | undefined): item is ColumnRef =>
      !!item;

    const dbtModelIdElements = dbtModelId.split('.');
    if (dbtModelIdElements.length !== 3)
      throw new RangeError('Unexpected number of dbt model id elements');

    const dbtModelIdProjectRoot = `${dbtModelIdElements[0]}.${dbtModelIdElements[1]}`;

    const wildcardDependencies = await this.#getDependenciesForWildcard(
      statementRefs,
      selfRef,
      dbtModelIdProjectRoot
    );

    const columnDependencies = statementRefs.columns
      .map((columnRef) => {
        const isDependency = this.#isDependencyOfTarget(columnRef, selfRef);
        if (isDependency) return columnRef;
        return undefined;
      })
      .filter(isColumnRef);

    const dependencies: ColumnRef[] =
      wildcardDependencies.concat(columnDependencies);

    await Promise.all(
      dependencies.map(async (dependency) => {
        if (!this.#lineage)
          throw new ReferenceError('Lineage property is undefined');

        if (this.#columnRefIsEqual(dependency, this.#lastQueryDependency))
          return;

        if (dependency.dependencyType === DependencyType.QUERY)
          this.#lastQueryDependency = dependency;
        await this.#createDependency.execute(
          {
            selfRef,
            parentRef: dependency,
            selfDbtModelId: dbtModelId,
            parentDbtModelIds: this.#logics.map(
              (element) => element.dbtModelId
            ),
            lineageId: this.#lineage.id,
          },
          { organizationId: 'todo' }
        );
      })
    );
  };

  /* Creates all dependencies that exist between DWH resources */
  #buildDependencies = async (): Promise<void> => {
    // todo - should method be completely sync? Probably resolves once transformed into batch job.

    await Promise.all(
      this.#logics.map(async (logic) => {
        await Promise.all(
          logic.statementRefs.map(async (refs) => {
            const dataDependencyRefs = this.#getDataDependencyRefs(refs);

            await Promise.all(
              dataDependencyRefs.map(async (selfRef) =>
                this.#buildDependency(selfRef, refs, logic.dbtModelId)
              )
            );
          })
        );
      })
    );
  };

  #getDependenciesForWildcard = async (
    statementRefs: Refs,
    selfRef: ColumnRef,
    dbtModelIdProjectRoot: string
  ): Promise<ColumnRef[]> => {
    const dbtIdProjectRoot =
      dbtModelIdProjectRoot.slice(-1) !== '.'
        ? dbtModelIdProjectRoot
        : dbtModelIdProjectRoot.substring(0, dbtModelIdProjectRoot.length - 1);

    const dependencies: ColumnRef[] = [];
    await Promise.all(
      statementRefs.wildcards.map(async (wildcardRef) => {
        if (!this.#lineage)
          throw new ReferenceError('Lineage property is undefined');

        const readColumnsResult = await this.#readColumns.execute(
          {
            dbtModelId: `${dbtIdProjectRoot}.${wildcardRef.materializationName}`,
            lineageId: this.#lineage.id,
          },
          { organizationId: 'todo' }
        );

        if (!readColumnsResult.success)
          throw new Error(readColumnsResult.error);
        if (!readColumnsResult.value)
          throw new ReferenceError(`Reading of columns failed`);

        const colsFromWildcard = readColumnsResult.value;

        colsFromWildcard.forEach((column) => {
          const newColumnRef: ColumnRef = {
            materializationName: wildcardRef.materializationName,
            path: wildcardRef.path,
            dependencyType: wildcardRef.dependencyType,
            isWildcardRef: wildcardRef.isWildcardRef,
            name: column.name,
            alias: wildcardRef.alias,
            schemaName: wildcardRef.schemaName,
            databaseName: wildcardRef.databaseName,
            warehouseName: wildcardRef.warehouseName,
          };

          const isDependency = this.#isDependencyOfTarget(
            newColumnRef,
            selfRef
          );
          if (isDependency) dependencies.push(newColumnRef);
        });
      })
    );

    return dependencies;
  };

  async execute(
    request: CreateLineageRequestDto,
    auth: CreateLineageAuthDto
  ): Promise<CreateLineageResponseDto> {
    // todo-replace
    console.log(auth);
    try {
      // todo - Workaround. Fix ioc container
      this.#lineage = undefined;
      this.#logics = [];
      this.#lastQueryDependency = undefined;

      await this.#buildLineage(request.lineageId, request.lineageCreatedAt);

      await this.#generateWarehouseResources();

      await this.#buildDependencies();

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      // todo - how to avoid checking if property exists. A sub-method created the property
      if (!this.#lineage)
        throw new ReferenceError('Lineage property is undefined');

      return Result.ok(buildLineageDto(this.#lineage));
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
