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
import BaseGetSfDataEnv from './base-get-sf-data-env';
import { QuerySnowflake } from '../snowflake-api/query-snowflake';

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
      RefreshSfDataEnvAuthDto
    >
{
  readonly #lineageRepo: ILineageRepo;

  readonly #materializationRepo: IMaterializationRepo;

  readonly #columnRepo: IColumnRepo;

  readonly #logicRepo: ILogicRepo;

  #logicsToHandle?: Logic[];

  #oldLogics: Logic[] = [];

  readonly #logicsToReplace: Logic[] = [];

  readonly #logicsToCreate: Logic[] = [];

  readonly #logicsToRemove: Logic[] = [];

  #matsToHandle?: Materialization[];

  readonly #matsToReplace: Materialization[] = [];

  readonly #matsToCreate: Materialization[] = [];

  readonly #matsToRemove: Materialization[] = [];

  #columnsToHandleByMatId?: { [key: string]: Column[] };

  #oldColumnsByMatId: { [key: string]: Column[] } = {};

  readonly #columnsToReplace: Column[] = [];

  readonly #columnsToCreate: Column[] = [];

  readonly #columnsToRemove: Column[] = [];

  #targetOrgId?: string;

  #auth?: RefreshSfDataEnvAuthDto;

  #connPool?: IConnectionPool;

  constructor(
    lineageRepo: ILineageRepo,
    materializationRepo: IMaterializationRepo,
    columnRepo: IColumnRepo,
    logicRepo: ILogicRepo,
    querySnowflake: QuerySnowflake
  ) {
    super(querySnowflake);
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

  static #groupByMatId = <T extends { materializationId: string }>(
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

  #mergeMatColumns = async (
    matToHandleId: string,
    oldMatId: string
  ): Promise<{ columnsToReplace: Column[]; columnsToCreate: Column[] }> => {
    const columnsToReplace: Column[] = [];
    const columnsToCreate: Column[] = [];

    await Promise.all(
      this.#columnsToHandleByMatId[matToHandleId].map(
        async (columnToHandle) => {
          const matchingColumn = this.#oldColumnsByMatId[oldMatId].find(
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
        }
      )
    );

    return { columnsToCreate, columnsToReplace };
  };

  #mergeMatLogic = async (
    logicToHandleId: string,
    oldLogicId: string
  ): Promise<Logic> => {
    const logicToHandle = this.#logicsToHandle.find(
      (logic) => logic.id === logicToHandleId
    );
    const oldLogic = this.#oldLogics.find((logic) => logic.id === oldLogicId);

    if (!logicToHandle || !oldLogic)
      throw new Error(
        'While merging logics an error occured. Error: Logic(s) not found'
      );

    const updatedLogic = this.#buildLogicToReplace(
      {
        id: oldLogic.id,
        lineageIds: oldLogic.lineageIds,
      },
      logicToHandle
    );

    return updatedLogic;
  };

  #handleNewMat = (mat: Materialization): void => {
    this.#matsToCreate.push(mat);
    this.#columnsToCreate.push(...this.#columnsToHandleByMatId[mat.id]);

    if (!mat.logicId) return;

    const logic = this.#logicsToHandle.find((el) => el.id === mat.logicId);
    if (!logic) throw new Error(`Missing logic for logic id ${mat.logicId}`);
    this.#logicsToCreate.push(logic);
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

  #addResources = async (addedMatIds: string[]): Promise<void> => {
    await Promise.all(addedMatIds.map(async(el) => {

      


    }));
    

  };

  #removeResources = async (addedMatIds: string[]): Promise<void> => {
    throw new Error('Not implemented');
  };

  #modifyResources = async (addedMatIds: string[]): Promise<void> => {
    throw new Error('Not implemented');
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

    await this.#addResources(dataEnvDiff.addedMatIds);
    await this.#removeResources(dataEnvDiff.removedMatIds);
    await this.#modifyResources(dataEnvDiff.modifiedMatIds);
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
        dbRepresentations.map(async (el) => refreshDbDataEnv())
      );

      const oldMats = await this.#materializationRepo.all(
        auth,
        connPool,
        this.#targetOrgId
      );

      this.#oldColumnsByMatId = (
        await this.#columnRepo.all(auth, connPool, this.#targetOrgId)
      ).reduce(RefreshSfDataEnv.#groupByMatId, {});

      this.#oldLogics = await this.#logicRepo.all(
        auth,
        connPool,
        this.#targetOrgId
      );

      await Promise.all(
        this.#matsToHandle.map(async (matToHandle) => {
          const matchingMat = oldMats.find(
            (oldMat) => matToHandle.relationName === oldMat.relationName
          );

          if (!matchingMat) {
            this.#handleNewMat(matToHandle);
            return;
          }

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

          const { columnsToCreate, columnsToReplace } =
            await this.#mergeMatColumns(matToHandle.id, matchingMat.id);

          this.#columnsToCreate.push(...columnsToCreate);
          this.#columnsToReplace.push(...columnsToReplace);

          if (matToHandle.logicId && matchingMat.logicId) {
            const logic = await this.#mergeMatLogic(
              matToHandle.logicId,
              matchingMat.logicId
            );

            this.#logicsToReplace.push(logic);
          } else if (!matToHandle.logicId && matchingMat.logicId)
            console.warn(
              `Mat to handle ${matToHandle.id} that is matching old mat ${matchingMat.id} with logic id ${matchingMat.logicId} is missing logic id`
            );
        })
      );

      return Result.ok({
        matsToCreate: this.#matsToCreate,
        matsToReplace: this.#matsToReplace,
        matsToRemove: this.#matsToRemove,
        columnsToCreate: this.#columnsToCreate,
        columnsToReplace: this.#columnsToReplace,
        columnsToRemove: this.#columnsToRemove,
        logicsToCreate: this.#logicsToCreate,
        logicsToReplace: this.#logicsToReplace,
        logicsToRemove: this.#logicsToRemove,
      });
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }

  abstract 
}
