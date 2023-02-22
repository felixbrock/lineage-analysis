import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import BaseAuth from '../services/base-auth';
import {
  ColToDeleteRef,
  DataEnvProps,
  LogicToDeleteRef,
  MatToDeleteRef,
} from './data-env';
import { Column } from '../entities/column';
import { Materialization } from '../entities/materialization';
import { Logic } from '../entities/logic';
import {
  IConnectionPool,
  SnowflakeEntity,
} from '../snowflake-api/i-snowflake-api-repo';
import { ILineageRepo } from '../lineage/i-lineage-repo';
import { IMaterializationRepo } from '../materialization/i-materialization-repo';
import { IColumnRepo } from '../column/i-column-repo';
import { ILogicRepo } from '../logic/i-logic-repo';
import BaseGetSfDataEnv, { ColumnRepresentation } from './base-get-sf-data-env';
import { QuerySnowflake } from '../snowflake-api/query-snowflake';
import { CreateMaterialization } from '../materialization/create-materialization';
import { CreateColumn } from '../column/create-column';
import { CreateLogic } from '../logic/create-logic';
import { ParseSQL } from '../sql-parser-api/parse-sql';

export interface UpdateSfDataEnvRequestDto {
  latestCompletedLineage: {
    completedAt: string;
    dbCoveredNames: string[];
  };
}

export interface UpdateSfDataEnvAuthDto extends Omit<BaseAuth, 'callerOrgId'> {
  callerOrgId: string;
}

export type UpdateSfDataEnvResponse = Result<DataEnvProps>;

interface MatModifiedDiff {
  oldMatId: string;
  relationName: string;
}

interface DataEnvDiff {
  matAddedRelationNames: string[];
  matDeletedIds: string[];
  matModifiedDiffs: MatModifiedDiff[];
}

export class UpdateSfDataEnv
  extends BaseGetSfDataEnv
  implements
    IUseCase<
      UpdateSfDataEnvRequestDto,
      UpdateSfDataEnvResponse,
      UpdateSfDataEnvAuthDto,
      IConnectionPool
    >
{
  readonly #lineageRepo: ILineageRepo;

  readonly #materializationRepo: IMaterializationRepo;

  readonly #columnRepo: IColumnRepo;

  readonly #logicRepo: ILogicRepo;

  readonly #logicsToReplace: Logic[] = [];

  readonly #logicsToCreate: Logic[] = [];

  readonly #logicToDeleteRefs: LogicToDeleteRef[] = [];

  readonly #matsToReplace: Materialization[] = [];

  readonly #matsToCreate: Materialization[] = [];

  readonly #matToDeleteRefs: MatToDeleteRef[] = [];

  readonly #columnsToReplace: Column[] = [];

  readonly #columnsToCreate: Column[] = [];

  readonly #columnToDeleteRefs: ColToDeleteRef[] = [];

  constructor(
    lineageRepo: ILineageRepo,
    materializationRepo: IMaterializationRepo,
    columnRepo: IColumnRepo,
    logicRepo: ILogicRepo,
    querySnowflake: QuerySnowflake,
    createMaterialization: CreateMaterialization,
    createColumn: CreateColumn,
    createLogic: CreateLogic,
    parseSQL: ParseSQL
  ) {
    super(
      createMaterialization,
      createColumn,
      createLogic,
      querySnowflake,
      parseSQL
    );
    this.#lineageRepo = lineageRepo;
    this.#materializationRepo = materializationRepo;
    this.#columnRepo = columnRepo;
    this.#logicRepo = logicRepo;
  }

  #buildLogicToReplace = (
    oldLogicProps: { id: string },
    logicToHandle: Logic
  ): Logic =>
    Logic.build({
      ...logicToHandle.toDto(),
      id: oldLogicProps.id,
    });

  #buildColumnToReplace = (
    oldColumnProps: {
      id: string;
      name: string;
      materializationId: string;
    },
    columnToHandle: Column
  ): Column => {
    const column = Column.build({
      ...columnToHandle.toDto(),
      id: oldColumnProps.id,
      name: oldColumnProps.name,
      materializationId: oldColumnProps.materializationId,
    });

    return column;
  };

  #buildMatToReplace = (
    oldMatProps: {
      id: string;
      relationName: string;
      logicId?: string;
    },
    matToHandle: Materialization
  ): Materialization => {
    const mat = Materialization.build({
      ...matToHandle.toDto(),
      id: oldMatProps.id,
      relationName: oldMatProps.relationName,
      logicId: oldMatProps.logicId || matToHandle.logicId,
    });

    return mat;
  };

  #mergeMatColumns = async (
    newCols: Column[],
    oldCols: Column[]
  ): Promise<{
    columnsToReplace: Column[];
    columnsToCreate: Column[];
    columnToDeleteRefs: ColToDeleteRef[];
  }> => {
    const columnsToReplace: Column[] = [];
    const columnsToCreate: Column[] = [];

    await Promise.all(
      newCols.map(async (columnToHandle) => {
        const matchingColumn = oldCols.find(
          (oldColumn) => oldColumn.name === columnToHandle.name
        );

        if (matchingColumn) {
          const updatedColumn = await this.#buildColumnToReplace(
            {
              id: matchingColumn.id,
              name: matchingColumn.name,
              materializationId: matchingColumn.materializationId,
            },
            columnToHandle
          );

          columnsToReplace.push(updatedColumn);
        } else columnsToCreate.push(columnToHandle);
      })
    );

    const isString = (obj: unknown): obj is ColToDeleteRef =>
      !!obj && typeof obj === 'object' && 'relationName' in obj;

    const colToDeleteRefs = oldCols
      .map((oldCol): ColToDeleteRef | undefined => {
        const matchingColumn = newCols.find(
          (newCol) => newCol.name === oldCol.name
        );

        if (!matchingColumn)
          return {
            id: oldCol.id,
            matId: oldCol.materializationId,
            name: oldCol.name,
            relationName: oldCol.relationName,
          };
        return undefined;
      })
      .filter(isString);

    return {
      columnsToCreate,
      columnsToReplace,
      columnToDeleteRefs: colToDeleteRefs,
    };
  };

  #mergeMatLogic = async (newLogic: Logic, oldLogic: Logic): Promise<Logic> => {
    const updatedLogic = this.#buildLogicToReplace(
      {
        id: oldLogic.id,
      },
      newLogic
    );

    return updatedLogic;
  };

  #getDataEnvDiff = (values: SnowflakeEntity[]): DataEnvDiff => {
    const isOptionalOfType = <T>(
      val: unknown,
      targetType:
        | 'string'
        | 'number'
        | 'bigint'
        | 'boolean'
        | 'symbol'
        | 'undefined'
        | 'object'
        | 'function'
    ): val is T => val === null || typeof val === targetType;

    const dataEnvDiff = values.reduce(
      (accumulation: DataEnvDiff, el: SnowflakeEntity): DataEnvDiff => {
        const localAcc = accumulation;

        const {
          MAT_DELETED_ID: matDeletedId,
          MAT_ADDED_RELATION_NAME: matAddedRelationName,
          ALTERED: altered,
        } = el;

        if (typeof altered !== 'boolean')
          throw new Error('Unexpected altered value');
        if (!isOptionalOfType<string>(matDeletedId, 'string'))
          throw new Error('Unexpected deletedMatId value');
        if (!isOptionalOfType<string>(matAddedRelationName, 'string'))
          throw new Error('Unexpected addedMatId value');

        if (altered) {
          if (
            typeof matAddedRelationName !== 'string' ||
            typeof matDeletedId !== 'string'
          )
            throw new Error('Unexpected altered column input');
          localAcc.matModifiedDiffs.push({
            oldMatId: matDeletedId,
            relationName: matAddedRelationName,
          });
        } else if (matAddedRelationName)
          localAcc.matAddedRelationNames.push(matAddedRelationName);
        else if (matDeletedId) localAcc.matDeletedIds.push(matDeletedId);
        else throw new Error('Unhandled use case returned');

        return localAcc;
      },

      {
        matDeletedIds: [],
        matAddedRelationNames: [],
        matModifiedDiffs: [],
      }
    );

    return dataEnvDiff;
  };

  #addResourcesToAdd = async (
    relationNames: string[],
    dbName: string
  ): Promise<void> => {
    if (!relationNames.length) return;

    const whereCondition = `array_contains(concat(table_catalog, '.', table_schema, '.', table_name)::variant, array_construct(${relationNames
      .map(() => `?`)
      .join(', ')}))`;
    const binds = relationNames;
    const matRepresentations = await this.getMatRepresentations(
      dbName,
      whereCondition,
      binds
    );
    const columnRepresentations = await this.getColumnRepresentations(
      dbName,
      whereCondition,
      binds
    );

    const colRepresentationsByRelationName: {
      [key: string]: ColumnRepresentation[];
    } = columnRepresentations.reduce(this.groupByRelationName, {});

    this.generateCatalog(matRepresentations, colRepresentationsByRelationName);

    await Promise.all(
      matRepresentations.map(async (el) => {
        const options = {
          writeToPersistence: false,
        };

        const logicRepresentation = await this.getLogicRepresentation(
          el.type === 'view' ? 'view' : 'table',
          el.name,
          el.schemaName,
          el.databaseName
        );

        const resourcesToCreate = await this.generateDWResource(
          {
            matRepresentation: el,
            logicRepresentation,
            columnRepresentations:
              colRepresentationsByRelationName[el.relationName],
            relationName: el.relationName,
          },
          options
        );

        this.#matsToCreate.push(resourcesToCreate.matToCreate);
        this.#columnsToCreate.push(...resourcesToCreate.colsToCreate);
        this.#logicsToCreate.push(resourcesToCreate.logicToCreate);
      })
    );
  };

  #groupByMatId = <T extends { materializationId: string }>(
    accumulation: { [key: string]: T[] },
    element: T
  ): { [key: string]: T[] } => {
    const localAcc = accumulation;

    const key = element.materializationId;
    if (!(key in accumulation)) {
      localAcc[key] = [];
    }
    localAcc[key].push(element);
    return localAcc;
  };

  #addResourcesToModify = async (
    modifiedMatDiffs: MatModifiedDiff[],
    dbName: string
  ): Promise<void> => {
    if (!this.auth || !this.connPool)
      throw new Error('auth or connPool missing');

    if (!modifiedMatDiffs.length) return;

    const { ids, relationNames } = modifiedMatDiffs.reduce(
      (
        accumulation: { relationNames: string[]; ids: string[] },
        el: MatModifiedDiff
      ): { relationNames: string[]; ids: string[] } => {
        const localAcc = accumulation;

        localAcc.ids.push(el.oldMatId);
        localAcc.relationNames.push(el.relationName);

        return localAcc;
      },
      { relationNames: [], ids: [] }
    );

    const oldMats = await this.#materializationRepo.findBy(
      { ids },
      this.auth,
      this.connPool
    );

    const oldCols = await this.#columnRepo.findBy(
      { materializationIds: ids },
      this.auth,
      this.connPool
    );

    const oldColsByMatId: { [key: string]: Column[] } = oldCols.reduce(
      this.#groupByMatId,
      {}
    );

    const oldLogics = await this.#logicRepo.findBy(
      { relationNames },
      this.auth,
      this.connPool
    );

    const whereCondition = `array_contains(concat(table_catalog, '.', table_schema, '.', table_name)::variant, array_construct(${relationNames
      .map(() => `?`)
      .join(', ')}))`;
    const binds = relationNames;
    const newMatReps = await this.getMatRepresentations(
      dbName,
      whereCondition,
      binds
    );
    const newColReps = await this.getColumnRepresentations(
      dbName,
      whereCondition,
      binds
    );

    const colRepresentationsByRelationName: {
      [key: string]: ColumnRepresentation[];
    } = newColReps.reduce(this.groupByRelationName, {});

    await Promise.all(
      newMatReps.map(async (el) => {
        const options = {
          writeToPersistence: false,
        };

        const newLogicRep = await this.getLogicRepresentation(
          el.type === 'view' ? 'view' : 'table',
          el.name,
          el.schemaName,
          el.databaseName
        );

        const {
          matToCreate: matToHandle,
          colsToCreate: colsToHandle,
          logicToCreate: logicToHandle,
        } = await this.generateDWResource(
          {
            matRepresentation: el,
            logicRepresentation: newLogicRep,
            columnRepresentations:
              colRepresentationsByRelationName[el.relationName],
            relationName: el.relationName,
          },
          options
        );

        const matchingMat = oldMats.find(
          (oldMat) => matToHandle.relationName === oldMat.relationName
        );

        if (!matchingMat) throw new Error('No mat to modify found');

        const updatedMat = await this.#buildMatToReplace(
          {
            id: matchingMat.id,
            logicId:
              matToHandle.logicId && !matchingMat.logicId
                ? matToHandle.logicId
                : matchingMat.logicId,
            relationName: matchingMat.relationName,
          },
          matToHandle
        );
        this.#matsToReplace.push(updatedMat);

        const { columnsToCreate, columnsToReplace, columnToDeleteRefs } =
          await this.#mergeMatColumns(
            colsToHandle,
            oldColsByMatId[matchingMat.id]
          );

        this.#columnsToCreate.push(...columnsToCreate);
        this.#columnsToReplace.push(...columnsToReplace);
        this.#columnToDeleteRefs.push(...columnToDeleteRefs);

        const oldLogic = oldLogics.find(
          (element) => element.id === matchingMat.logicId
        );
        if (!oldLogic) throw new Error('Old logic not found');

        if (matToHandle.logicId && matchingMat.logicId) {
          const logic = await this.#mergeMatLogic(logicToHandle, oldLogic);

          this.#logicsToReplace.push(logic);
        } else if (!matToHandle.logicId && matchingMat.logicId)
          console.warn(
            `Mat to handle ${matToHandle.id} that is matching old mat ${matchingMat.id} with logic id ${matchingMat.logicId} is missing logic id`
          );
      })
    );
  };

  #addResourcesToDelete = async (props: {
    matToDeleteIds: string[];
    matsToDelete?: Materialization[];
  }): Promise<void> => {
    if (!this.auth || !this.connPool)
      throw new Error('auth or connPool missing');

    if (!props.matToDeleteIds.length) return;

    const mats =
      props.matsToDelete ||
      (await this.#materializationRepo.findBy(
        { ids: props.matToDeleteIds },
        this.auth,
        this.connPool
      ));

    if (!mats.length) throw new Error('Desired mats not found');

    const cols = await this.#columnRepo.findBy(
      { materializationIds: props.matToDeleteIds },
      this.auth,
      this.connPool
    );

    const logics = await this.#logicRepo.findBy(
      { relationNames: mats.map((el) => el.relationName) },
      this.auth,
      this.connPool
    );

    this.#matToDeleteRefs.push(
      ...mats.map(
        (el): MatToDeleteRef => ({
          id: el.id,
          name: el.name,
          dbName: el.databaseName,
          schemaName: el.schemaName,
        })
      )
    );
    this.#columnToDeleteRefs.push(
      ...cols.map(
        (el): ColToDeleteRef => ({
          id: el.id,
          matId: el.materializationId,
          name: el.name,
          relationName: el.relationName,
        })
      )
    );
    this.#logicToDeleteRefs.push(
      ...logics.map(
        (el): LogicToDeleteRef => ({ id: el.id, relationName: el.relationName })
      )
    );
  };

  #updateDbDataEnv = async (
    dbName: string,
    lastLineageCompletedAt: string
  ): Promise<void> => {
    if (!this.auth || !this.connPool)
      throw new Error('Missing auth or connPool');

    const binds = [dbName, lastLineageCompletedAt];

    const queryText = `select t2.id as mat_deleted_id, concat(t1.table_catalog, '.', t1.table_schema, '.', t1.table_name) as mat_added_relation_name, t1.table_name is not null and t2.relation_name is not null as altered
    from cito.lineage.materializations as t2
    full join "${dbName}".information_schema.tables as t1 
    on concat(t1.table_catalog, '.', t1.table_schema, '.', t1.table_name) = t2.relation_name
    where 
    (
        array_contains(t2.database_name::variant, array_construct(:1, null))
        and
        array_contains(t1.table_catalog::variant, array_construct(:1, null))
    ) 
    and 
    (
        (
            t1.table_name is null 
            or 
            (t2.relation_name is null and t1.table_schema != 'INFORMATION_SCHEMA')
        )
        or 
        timediff(minute, :2::timestamp_ntz, convert_timezone('UTC', last_altered)::timestamp_ntz) > 0
    );`;

    const queryResult = await this.querySnowflake.execute(
      { queryText, binds },
      this.auth,
      this.connPool
    );

    if (!queryResult.success) throw new Error(queryResult.error);
    if (!queryResult.value)
      throw new Error('Query result is missing value field');

    const dataEnvDiff = this.#getDataEnvDiff(queryResult.value);

    await this.#addResourcesToAdd(dataEnvDiff.matAddedRelationNames, dbName);
    await this.#addResourcesToDelete({
      matToDeleteIds: dataEnvDiff.matDeletedIds,
    });
    await this.#addResourcesToModify(dataEnvDiff.matModifiedDiffs, dbName);
  };

  #addRemovedDbToDelete = async (dbName: string): Promise<void> => {
    if (!this.auth || !this.connPool)
      throw new Error('Missing auth or connPool');

    const matsToDelete = await this.#materializationRepo.findBy(
      { databaseName: dbName },
      this.auth,
      this.connPool
    );

    const matIds = matsToDelete.map((el) => el.id);

    await this.#addResourcesToDelete({ matToDeleteIds: matIds, matsToDelete });
  };

  /* Checks Snowflake resources for changes and returns partial data env to merge with existing snapshot */
  async execute(
    req: UpdateSfDataEnvRequestDto,
    auth: UpdateSfDataEnvAuthDto,
    connPool: IConnectionPool
  ): Promise<UpdateSfDataEnvResponse> {
    try {
      this.auth = auth;
      this.connPool = connPool;

      const dbRepresentations = await this.getDbRepresentations(connPool, auth);

      const dbToCoverNames = dbRepresentations.map((el) => el.name);
      const dbRemovedNames = req.latestCompletedLineage.dbCoveredNames.filter(
        (dbOldName) => !dbToCoverNames.includes(dbOldName)
      );

      await Promise.all(
        dbRemovedNames.map(async (el) => {
          await this.#addRemovedDbToDelete(el);
        })
      );

      await Promise.all(
        dbRepresentations.map(async (el) => {
          await this.#updateDbDataEnv(
            el.name,
            req.latestCompletedLineage.completedAt
          );
        })
      );

      return Result.ok({
        dataEnv: {
          matsToCreate: this.#matsToCreate,
          matsToReplace: this.#matsToReplace,
          matToDeleteRefs: this.#matToDeleteRefs.map((el) => ({
            id: el.id,
            name: el.name,
            schemaName: el.schemaName,
            dbName: el.dbName,
          })),
          columnsToCreate: this.#columnsToCreate,
          columnsToReplace: this.#columnsToReplace,
          columnToDeleteRefs: this.#columnToDeleteRefs.map((el) => ({
            id: el.id,
            name: el.name,
            relationName: el.relationName,
            matId: el.matId,
          })),
          logicsToCreate: this.#logicsToCreate,
          logicsToReplace: this.#logicsToReplace,
          logicToDeleteRefs: this.#logicToDeleteRefs.map((el) => ({
            id: el.id,
            relationName: el.relationName,
          })),
        },
        catalog: this.catalog,
        dbCoveredNames: dbRepresentations.map((el) => el.name),
      });
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
