import { CreateColumn } from '../../column/create-column';
import { IColumnRepo } from '../../column/i-column-repo';
import { Column } from '../../entities/column';
import { Logic } from '../../entities/logic';
import { Materialization } from '../../entities/materialization';
import { ILogicRepo } from '../../logic/i-logic-repo';
import { CreateMaterialization } from '../../materialization/create-materialization';
import { IMaterializationRepo } from '../../materialization/i-materialization-repo';
import { DbConnection } from '../../services/i-db';
import { ILineageRepo } from '../i-lineage-repo';

interface Auth {
  callerOrganizationId?: string;
  isSystemInternal: boolean;
}

export default class DataEnvMerger {
  readonly #lineageRepo: ILineageRepo;

  readonly #createMaterialization: CreateMaterialization;

  readonly #createColumn: CreateColumn;

  readonly #materializationRepo: IMaterializationRepo;

  readonly #columnRepo: IColumnRepo;

  readonly #logicRepo: ILogicRepo;

  readonly #dbConnection: DbConnection;

  readonly #auth: Auth;

  readonly #organizationId: string;

  #logicsToHandle: Logic[];

  #oldLogics: Logic[] = [];

  #matsToHandle: Materialization[];

  #columnsToHandle: { [key: string]: Column[] };

  #oldColumns: { [key: string]: Column[] } = {};

  constructor(
    props: {
      organizationId: string;
      materializations: Materialization[];
      columns: Column[];
      logics: Logic[];
    },
    auth: Auth,
    dbConnection: DbConnection,
    dependencies: {
      lineageRepo: ILineageRepo;
      createMaterialization: CreateMaterialization;
      createColumn: CreateColumn;
      materializationRepo: IMaterializationRepo;
      columnRepo: IColumnRepo;
      logicRepo: ILogicRepo;
    }
  ) {
    this.#lineageRepo = dependencies.lineageRepo;
    this.#createMaterialization = dependencies.createMaterialization;
    this.#createColumn = dependencies.createColumn;
    this.#materializationRepo = dependencies.materializationRepo;
    this.#columnRepo = dependencies.columnRepo;
    this.#logicRepo = dependencies.logicRepo;

    this.#dbConnection = dbConnection;
    this.#auth = auth;

    this.#organizationId = props.organizationId;
    this.#matsToHandle = props.materializations;
    this.#columnsToHandle = props.columns.reduce(
      DataEnvMerger.#groupByMatId,
      {}
    );
    this.#logicsToHandle = props.logics;
  }

  #buildLogicToUpdate = async (
    oldLogicProps: { id: string; parsedLogic: string; lineageIds: string[] },
    logicToHandle: Logic
  ): Promise<Logic> =>
    Logic.build({
      ...logicToHandle.toDto(),
      id: oldLogicProps.id,
      parsedLogic: oldLogicProps.parsedLogic,
      lineageIds: oldLogicProps.lineageIds.concat(logicToHandle.lineageIds),
    });

  #buildColumnToUpdate = async (
    oldColumnProps: { id: string; name: string; lineageIds: string[] },
    columnToHandle: Column
  ): Promise<Column> => {
    const createColumnResult = await this.#createColumn.execute(
      {
        ...columnToHandle.toDto(),
        id: oldColumnProps.id,
        name: oldColumnProps.name,
        lineageIds: oldColumnProps.lineageIds.concat(columnToHandle.lineageIds),
        writeToPersistence: false,
      },
      this.#auth,
      this.#dbConnection
    );

    if (!createColumnResult.success) throw new Error(createColumnResult.error);
    if (!createColumnResult.value)
      throw new Error('Create Column failed - Unknown error');

    return createColumnResult.value;
  };

  #buildMatToUpdate = async (
    oldMatProps: { id: string; relationName: string; lineageIds: string[] },
    matToHandle: Materialization
  ): Promise<Materialization> => {
    const createMatResult = await this.#createMaterialization.execute(
      {
        ...matToHandle.toDto(),
        id: oldMatProps.id,
        relationName: oldMatProps.relationName,
        lineageIds: oldMatProps.lineageIds.concat(matToHandle.lineageIds),
        writeToPersistence: false,
      },
      this.#auth,
      this.#dbConnection
    );

    if (!createMatResult.success) throw new Error(createMatResult.error);
    if (!createMatResult.value)
      throw new Error('Create Mat failed - Unknown error');

    return createMatResult.value;
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
  ): Promise<{ columnsToUpdate: Column[]; columnsToCreate: Column[] }> => {
    const columnsToUpdate: Column[] = [];
    const columnsToCreate: Column[] = [];

    await Promise.all(
      this.#columnsToHandle[matToHandleId].map(async (columnToHandle) => {
        const matchingColumn = this.#oldColumns[oldMatId].find(
          (oldColumn) => oldColumn.name === columnToHandle.name
        );

        if (matchingColumn) {
          const updatedColumn = await this.#buildColumnToUpdate(
            {
              id: matchingColumn.id,
              name: matchingColumn.name,
              lineageIds: matchingColumn.lineageIds.concat(
                columnToHandle.lineageIds
              ),
            },
            columnToHandle
          );

          columnsToUpdate.push(updatedColumn);
        } else columnsToCreate.push(columnToHandle);
      })
    );

    return { columnsToCreate, columnsToUpdate };
  };

  #mergeMatLogic = async (
    logicToHandleId: string,
    oldLogicId: string
  ): Promise<{ logic: Logic; toUpdate: boolean }> => {
    const logicToHandle = this.#logicsToHandle.find(
      (logic) => logic.id === logicToHandleId
    );
    const oldLogic = this.#oldLogics.find((logic) => logic.id === oldLogicId);

    if (!logicToHandle || !oldLogic)
      throw new Error(
        'While merging logics an error occured. Error: Logic(s) not found'
      );

    if (logicToHandle.parsedLogic === oldLogic.parsedLogic) {
      const updatedLogic = await this.#buildLogicToUpdate(
        {
          id: oldLogic.id,
          parsedLogic: oldLogic.parsedLogic,
          lineageIds: oldLogic.lineageIds.concat(logicToHandle.lineageIds),
        },
        logicToHandle
      );

      return { logic: updatedLogic, toUpdate: true };
    }

    return { logic: logicToHandle, toUpdate: false };
  };

  merge = async (): Promise<void> => {
    // todo- needs to retrieve latest compeleted lineage
    const latestLineage = await this.#lineageRepo.findLatest(
      this.#dbConnection,
      this.#organizationId
    );

    if (!latestLineage) return;

    const oldMats = await this.#materializationRepo.findBy(
      { lineageIds: [latestLineage.id], organizationId: this.#organizationId },
      this.#dbConnection
    );

    const matsToUpdate: Materialization[] = [];
    const matsToCreate: Materialization[] = [];

    const columnsToUpdate: Column[] = [];
    const columnsToCreate: Column[] = [];

    const logicsToUpdate: Logic[] = [];
    const logicsToCreate: Logic[] = [];

    await Promise.all(
      this.#matsToHandle.map(async (matToHandle) => {
        const matchingMat = oldMats.find(
          (oldMat) => matToHandle.relationName === oldMat.relationName
        );

        if (!matchingMat) {
          matsToCreate.push(matToHandle);
          columnsToCreate.push(...this.#columnsToHandle[matToHandle.id]);

          if (!matToHandle.logicId) return;

          const logic = this.#logicsToHandle.find(
            (el) => el.id === matToHandle.logicId
          );
          if (!logic)
            throw new Error(
              `Missing logic for logic id ${matToHandle.logicId}`
            );
          logicsToCreate.push(logic);
          return;
        }

        const updatedMat = await this.#buildMatToUpdate(
          {
            lineageIds: matchingMat.lineageIds,
            id: matchingMat.id,
            relationName: matchingMat.relationName,
          },
          matToHandle
        );
        matsToUpdate.push(updatedMat);

        if (!Object.keys(this.#oldColumns).length)
          this.#oldColumns = (
            await this.#columnRepo.findBy(
              {
                lineageIds: [latestLineage.id],
                organizationId: this.#organizationId,
              },
              this.#dbConnection
            )
          ).reduce(DataEnvMerger.#groupByMatId, {});

        const { columnsToCreate: colsToCreate, columnsToUpdate: colsToUpdate } =
          await this.#mergeMatColumns(matToHandle.id, matchingMat.id);

        columnsToCreate.push(...colsToCreate);
        columnsToUpdate.push(...colsToUpdate);

        if (!Object.keys(this.#oldLogics).length)
          this.#oldLogics = await this.#logicRepo.findBy(
            {
              lineageIds: [latestLineage.id],
              organizationId: this.#organizationId,
            },
            this.#dbConnection
          );

        if (matToHandle.logicId && matchingMat.logicId) {
          const { logic, toUpdate } = await this.#mergeMatLogic(
            matToHandle.logicId,
            matchingMat.logicId
          );

          if (toUpdate) logicsToUpdate.push(logic);
          else {
            logicsToCreate.push(logic);
          }
        }
      })
    );
  };
}
