import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import BaseAuth from '../services/base-auth';
import { DataEnv } from './data-env';
import { Column } from '../entities/column';
import {
  Materialization,
} from '../entities/materialization';
import { Logic } from '../entities/logic';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import { ILineageRepo } from '../lineage/i-lineage-repo';
import { IMaterializationRepo } from '../materialization/i-materialization-repo';
import { IColumnRepo } from '../column/i-column-repo';
import { ILogicRepo } from '../logic/i-logic-repo';

export interface RefreshSfDataEnvRequestDto {
  targetOrgId?: string;
  latestLineageCompleted: string;
}

export type RefreshSfDataEnvAuthDto = BaseAuth;

export type RefreshSfDataEnvResponse = Result<DataEnv>;

export class RefreshSfDataEnv
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

  constructor(
    lineageRepo: ILineageRepo,
    materializationRepo: IMaterializationRepo,
    columnRepo: IColumnRepo,
    logicRepo: ILogicRepo
  ) {
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

  /* Checks Snowflake resources for changes and returns partial data env to merge with existing snapshot */
  async execute(
    req: RefreshSfDataEnvRequestDto,
    auth: RefreshSfDataEnvAuthDto,
    connPool: IConnectionPool
  ): Promise<RefreshSfDataEnvResponse> {
    try {
      SELECT t2.relation_name as removed_mat, lower(concat(t1.table_catalog, '.', t1.table_schema, '.', t1.table_name)) as added_mat, t1.table_name is not null and t2.relation_name is not null as altered 
      FROM cito.lineage.materializations as t2
        full JOIN test.information_schema.tables as t1 ON lower(concat(t1.table_catalog, '.', t1.table_schema, '.', t1.table_name)) = t2.relation_name
      WHERE t1.table_name IS NULL or (t2.relation_name is Null and t1.table_schema != 'INFORMATION_SCHEMA') or
      timediff(minute, convert_timezone('UTC', last_altered)::timestamp_ntz, sysdate()) < 30

      const latestLineage = await this.#lineageRepo.findLatest(
        { tolerateIncomplete: false },
        auth,
        connPool,
        this.#targetOrgId
      );

      const oldMats = await this.#materializationRepo.all(
        auth,
        connPool,
        this.#targetOrgId
      );

      this.#oldColumnsByMatId = (
        await this.#columnRepo.all(
          auth,
          connPool,
          this.#targetOrgId
        )
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
}
