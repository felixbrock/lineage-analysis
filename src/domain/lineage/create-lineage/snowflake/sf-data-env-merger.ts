import { IColumnRepo } from '../../../column/i-column-repo';
import { Column } from '../../../entities/column';
import { Logic } from '../../../entities/logic';
import { Materialization } from '../../../entities/materialization';
import { IMaterializationRepo } from '../../../materialization/i-materialization-repo';
import { ILineageRepo } from '../../i-lineage-repo';

export default class SnowflakeDataEnvMerger {
  readonly #lineageRepo: ILineageRepo;

  readonly #materializationRepo: IMaterializationRepo;

  readonly #columnRepo: IColumnRepo;

  readonly #organizationId: string;

  #matsToHandle: Materialization[];

  #matsToReplace: Materialization[] = [];

  get matsToReplace(): Materialization[] {
    return this.#matsToReplace;
  }

  #matsToCreate: Materialization[] = [];

  get matsToCreate(): Materialization[] {
    return this.#matsToCreate;
  }

  #columnsToHandleByMatId: { [key: string]: Column[] };

  #oldColumnsByMatId: { [key: string]: Column[] } = {};

  #columnsToReplace: Column[] = [];

  get columnsToReplace(): Column[] {
    return this.#columnsToReplace;
  }

  #columnsToCreate: Column[] = [];

  get columnsToCreate(): Column[] {
    return this.#columnsToCreate;
  }

  constructor(
    props: {
      organizationId: string;
      materializations: Materialization[];
      columns: Column[];
    },
    dependencies: {
      lineageRepo: ILineageRepo;
      materializationRepo: IMaterializationRepo;
      columnRepo: IColumnRepo;
    }
  ) {
    this.#lineageRepo = dependencies.lineageRepo;
    this.#materializationRepo = dependencies.materializationRepo;
    this.#columnRepo = dependencies.columnRepo;

    this.#organizationId = props.organizationId;
    this.#matsToHandle = props.materializations;
    this.#columnsToHandleByMatId = props.columns.reduce(
      SnowflakeDataEnvMerger.#groupByMatId,
      {}
    );
  }

  #buildLogicToReplace = async (
    oldLogicProps: { id: string; lineageIds: string[] },
    logicToHandle: Logic
  ): Promise<Logic> =>
    Logic.build({
      ...logicToHandle.toDto(),
      id: oldLogicProps.id,
      lineageIds: oldLogicProps.lineageIds.concat(logicToHandle.lineageIds),
    });

  #buildColumnToReplace = async (
    oldColumnProps: {
      id: string;
      name: string;
      materializationId: string;
      lineageIds: string[];
    },
    columnToHandle: Column
  ): Promise<Column> => {
    const column = Column.build({
      ...columnToHandle.toDto(),
      id: oldColumnProps.id,
      name: oldColumnProps.name,
      materializationId: oldColumnProps.materializationId,
      lineageIds: oldColumnProps.lineageIds.concat(columnToHandle.lineageIds),
    });

    return column;
  };

  #buildMatToReplace = async (
    oldMatProps: {
      id: string;
      relationName: string;
      logicId?: string;
      lineageIds: string[];
    },
    matToHandle: Materialization
  ): Promise<Materialization> => {
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

  merge = async (): Promise<{
    matsToCreate: Materialization[];
    matsToReplace: Materialization[];
    columnsToCreate: Column[];
    columnsToReplace: Column[];
  }> => {
    const latestLineage = await this.#lineageRepo.findLatest(

      { organizationId: this.#organizationId, completed: true }
    );

    if (!latestLineage)
      return {
        matsToCreate: this.#matsToHandle,
        matsToReplace: [],
        columnsToCreate: Object.values(this.#columnsToHandleByMatId).reduce(
          (acc, val) => acc.concat(val),
          []
        ),
        columnsToReplace: [],
      };

    const oldMats = await this.#materializationRepo.findBy(
      { lineageId: latestLineage.id, organizationId: this.#organizationId },
    );

    await Promise.all(
      this.#matsToHandle.map(async (matToHandle) => {
        const matchingMat = oldMats.find(
          (oldMat) => matToHandle.relationName === oldMat.relationName
        );

        if (!matchingMat) {
          this.#matsToCreate.push(matToHandle);
          this.#columnsToCreate.push(
            ...this.#columnsToHandleByMatId[matToHandle.id]
          );

          if (!matToHandle.logicId) return;
          /*




           todo - Placeholder for logic handling 
          
          
          
          
          
           */
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

        if (!Object.keys(this.#oldColumnsByMatId).length)
          this.#oldColumnsByMatId = (
            await this.#columnRepo.findBy(
              {
                lineageId: latestLineage.id,
                organizationId: this.#organizationId,
              },
            )
          ).reduce(SnowflakeDataEnvMerger.#groupByMatId, {});

        const { columnsToCreate, columnsToReplace } =
          await this.#mergeMatColumns(matToHandle.id, matchingMat.id);

        this.#columnsToCreate.push(...columnsToCreate);
        this.#columnsToReplace.push(...columnsToReplace);

        /* 


         todo - handle logic
        
        
         */

        /* 


         todo - handle logic
        
        
         */
      })
    );

    return {
      matsToCreate: this.#matsToCreate,
      matsToReplace: this.#matsToReplace,
      columnsToCreate: this.#columnsToCreate,
      columnsToReplace: this.#columnsToReplace,
    };
  };
}
