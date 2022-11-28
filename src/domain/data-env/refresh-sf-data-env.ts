import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import BaseAuth from '../services/base-auth';
import { DataEnv } from './data-env';
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

export interface RefreshSfDataEnvRequestDto {
  targetOrgId?: string;
  lastLineageCompletedTimestamp: string;
}

export type RefreshSfDataEnvAuthDto = BaseAuth;

export type RefreshSfDataEnvResponse = Result<DataEnv>;

interface DataEnvDiff {
  addedMatIds: string[];
  removedMatIds: string[];
  modifiedMatIds: string[];
}

export class RefreshSfDataEnv
  extends BaseGetSfDataEnv
  implements
    IUseCase<
      RefreshSfDataEnvRequestDto,
      RefreshSfDataEnvResponse,
      RefreshSfDataEnvAuthDto,
      IConnectionPool
    >
{
  readonly #lineageRepo: ILineageRepo;

  readonly #materializationRepo: IMaterializationRepo;

  readonly #columnRepo: IColumnRepo;

  readonly #logicRepo: ILogicRepo;

  // #logicsToHandle?: Logic[];

  // #oldLogics: Logic[] = [];

  readonly #logicsToReplace: Logic[] = [];

  readonly #logicsToCreate: Logic[] = [];

  readonly #logicIdsToRemove: string[] = [];

  // #matsToHandle?: Materialization[];

  readonly #matsToReplace: Materialization[] = [];

  readonly #matsToCreate: Materialization[] = [];

  readonly #matIdsToRemove: string[] = [];

  // #columnsToHandleByMatId?: { [key: string]: Column[] };

  // #oldColumnsByMatId: { [key: string]: Column[] } = {};

  readonly #columnsToReplace: Column[] = [];

  readonly #columnsToCreate: Column[] = [];

  readonly #columnIdsToRemove: string[] = [];

  #targetOrgId?: string;

  #auth?: RefreshSfDataEnvAuthDto;

  #connPool?: IConnectionPool;

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
    oldLogicProps: { id: string; lineageIds: string[] },
    logicToHandle: Logic
  ): Logic =>
    Logic.build({
      ...logicToHandle.toDto(),
      id: oldLogicProps.id,
      lineageIds: oldLogicProps.lineageIds.concat(logicToHandle.lineageIds),
    });

  #buildColumnToReplace = (
    oldColumnProps: {
      id: string;
      name: string;
      materializationId: string;
      lineageIds: string[];
    },
    columnToHandle: Column
  ): Column => {
    const column = Column.build({
      ...columnToHandle.toDto(),
      id: oldColumnProps.id,
      name: oldColumnProps.name,
      materializationId: oldColumnProps.materializationId,
      lineageIds: oldColumnProps.lineageIds.concat(columnToHandle.lineageIds),
    });

    return column;
  };

  #buildMatToReplace = (
    oldMatProps: {
      id: string;
      relationName: string;
      logicId?: string;
      lineageIds: string[];
    },
    matToHandle: Materialization
  ): Materialization => {
    const mat = Materialization.build({
      ...matToHandle.toDto(),
      id: oldMatProps.id,
      relationName: oldMatProps.relationName,
      logicId: oldMatProps.logicId || matToHandle.logicId,
      lineageIds: oldMatProps.lineageIds.concat(matToHandle.lineageIds),
    });

    return mat;
  };

  #mergeMatColumns = async (
    newCols: Column[],
    oldCols: Column[]
  ): Promise<{
    columnsToReplace: Column[];
    columnsToCreate: Column[];
    columnIdsToRemove: string[];
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
              lineageIds: matchingColumn.lineageIds,
            },
            columnToHandle
          );

          columnsToReplace.push(updatedColumn);
        } else columnsToCreate.push(columnToHandle);
      })
    );

    const isString = (obj: unknown): obj is string => typeof obj === 'string';

    const columnIdsToRemove = oldCols
      .map((oldCol) => {
        const matchingColumn = newCols.find(
          (newCol) => newCol.name === oldCol.name
        );

        if (!matchingColumn) return oldCol.id;
        return undefined;
      })
      .filter(isString);

    return { columnsToCreate, columnsToReplace, columnIdsToRemove };
  };

  #mergeMatLogic = async (newLogic: Logic, oldLogic: Logic): Promise<Logic> => {
    const updatedLogic = this.#buildLogicToReplace(
      {
        id: oldLogic.id,
        lineageIds: oldLogic.lineageIds,
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
          REMOVED_MAT_ID: removedMatId,
          ADDED_MAT_ID: addedMatId,
          ALTERED: altered,
        } = el;

        if (typeof altered !== 'boolean')
          throw new Error('Unexpected altered value');
        if (!isOptionalOfType<string>(removedMatId, 'string'))
          throw new Error('Unexpected removedMatId value');
        if (!isOptionalOfType<string>(addedMatId, 'string'))
          throw new Error('Unexpected addedMatId value');

        if (altered) {
          if (typeof addedMatId !== 'string')
            throw new Error('Unexpected added mat id value');
          localAcc.modifiedMatIds.push(addedMatId);
        } else if (addedMatId) localAcc.addedMatIds.push(addedMatId);
        else if (removedMatId) localAcc.removedMatIds.push(removedMatId);
        else throw new Error('Unhandled use case returned');

        return localAcc;
      },

      { addedMatIds: [], removedMatIds: [], modifiedMatIds: [] }
    );

    return dataEnvDiff;
  };

  #addResourcesToAdd = async (
    addedMatIds: string[],
    dbName: string
  ): Promise<void> => {
    const whereCondition = `array_contains(lower(table_name)::variant, array_construct(${addedMatIds
      .map(() => `?`)
      .join(', ')}))`;
    const binds = addedMatIds.map((el) => el.toLowerCase());
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
    modifiedMatIds: string[],
    dbName: string
  ): Promise<void> => {
    if (!this.#auth || !this.#connPool)
      throw new Error('auth or connPool missing');

    const oldMats = await this.#materializationRepo.findBy(
      { ids: modifiedMatIds },
      this.#auth,
      this.#connPool
    );

    const oldCols = await this.#columnRepo.findBy(
      { materializationIds: modifiedMatIds },
      this.#auth,
      this.#connPool
    );

    const oldColsByMatId: { [key: string]: Column[] } = oldCols.reduce(
      this.#groupByMatId,
      {}
    );

    const oldLogics = await this.#logicRepo.findBy(
      { materializationIds: modifiedMatIds },
      this.#auth,
      this.#connPool
    );

    const whereCondition = `array_contains(lower(table_name)::variant, array_construct(${modifiedMatIds
      .map(() => `?`)
      .join(', ')}))`;
    const binds = modifiedMatIds.map((el) => el.toLowerCase());
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
            lineageIds: matchingMat.lineageIds,
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

        const { columnsToCreate, columnsToReplace, columnIdsToRemove } =
          await this.#mergeMatColumns(
            colsToHandle,
            oldColsByMatId[matchingMat.id]
          );

        this.#columnsToCreate.push(...columnsToCreate);
        this.#columnsToReplace.push(...columnsToReplace);
        this.#columnIdsToRemove.push(...columnIdsToRemove);

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

  #addResourcesToRemove = async (removedMatIds: string[]): Promise<void> => {
    if (!this.#auth || !this.#connPool)
      throw new Error('auth or connPool missing');

    const columns = await this.#columnRepo.findBy(
      { materializationIds: removedMatIds },
      this.#auth,
      this.#connPool
    );

    const logics = await this.#logicRepo.findBy(
      { materializationIds: removedMatIds },
      this.#auth,
      this.#connPool
    );

    this.#matIdsToRemove.push(...removedMatIds);
    this.#columnIdsToRemove.push(...columns.map((el) => el.id));
    this.#logicIdsToRemove.push(...logics.map((el) => el.id));
  };

  #refreshDbDataEnv = async (
    dbName: string,
    lastLineageCompletedTimestamp: string
  ): Promise<void> => {
    if (!this.#auth || !this.#connPool)
      throw new Error('Missing auth or connPool');

    const binds = [lastLineageCompletedTimestamp];

    const queryText = `select t2.relation_name as removed_mat_id, lower(concat(t1.table_catalog, '.', t1.table_schema, '.', t1.table_name)) as added_mat_id, t1.table_name is not null and t2.relation_name is not null as altered
      from cito.lineage.materializations as t2
      full join ${dbName}.information_schema.tables as t1 
      on lower(concat(t1.table_catalog, '.', t1.table_schema, '.', t1.table_name)) = t2.relation_name
      where t1.table_name is null or (t2.relation_name is null and t1.table_schema != 'INFORMATION_SCHEMA') 
      or timediff(minute, ?::timestamp_ntz, convert_timezone('UTC', last_altered)::timestamp_ntz) > 0`;

    const queryResult = await this.querySnowflake.execute(
      { queryText, binds },
      this.#auth,
      this.#connPool
    );

    if (!queryResult.success) throw new Error(queryResult.error);
    if (!queryResult.value)
      throw new Error('Query result is missing value field');

    const dataEnvDiff = this.#getDataEnvDiff(queryResult.value);

    await this.#addResourcesToAdd(dataEnvDiff.addedMatIds, dbName);
    await this.#addResourcesToRemove(dataEnvDiff.removedMatIds);
    await this.#addResourcesToModify(dataEnvDiff.modifiedMatIds, dbName);
  };

  /* Checks Snowflake resources for changes and returns partial data env to merge with existing snapshot */
  async execute(
    req: RefreshSfDataEnvRequestDto,
    auth: RefreshSfDataEnvAuthDto,
    connPool: IConnectionPool
  ): Promise<RefreshSfDataEnvResponse> {
    try {
      const dbRepresentations = await this.getDbRepresentations(connPool, auth);

      await Promise.all(
        dbRepresentations.map(async (el) =>
          this.#refreshDbDataEnv(el.name, req.lastLineageCompletedTimestamp)
        )
      );

      return Result.ok({
        matsToCreate: this.#matsToCreate,
        matsToReplace: this.#matsToReplace,
        matsToRemove: this.#matIdsToRemove,
        columnsToCreate: this.#columnsToCreate,
        columnsToReplace: this.#columnsToReplace,
        columnsToRemove: this.#columnIdsToRemove,
        logicsToCreate: this.#logicsToCreate,
        logicsToReplace: this.#logicsToReplace,
        logicsToRemove: this.#logicIdsToRemove,
      });
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
