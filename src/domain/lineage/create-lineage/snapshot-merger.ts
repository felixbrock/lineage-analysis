import { CreateColumn } from '../../column/create-column';
import { IColumnRepo } from '../../column/i-column-repo';
import { Column } from '../../entities/column';
import { Logic } from '../../entities/logic';
import { Materialization } from '../../entities/materialization';
import { CreateMaterialization } from '../../materialization/create-materialization';
import { IMaterializationRepo } from '../../materialization/i-materialization-repo';
import { DbConnection } from '../../services/i-db';
import { ILineageRepo } from '../i-lineage-repo';

interface Auth {
  callerOrganizationId?: string;
  isSystemInternal: boolean;
}

class SnapshotMerger {
  readonly #lineageRepo: ILineageRepo;

  readonly #createMaterialization: CreateMaterialization;

  readonly #createColumn: CreateColumn;

  readonly #materializationRepo: IMaterializationRepo;

  readonly #columnRepo: IColumnRepo;

  readonly #dbConnection: DbConnection;

  readonly #auth: Auth;

  readonly #organizationId: string;

  #logicsToHandle: Logic[];

  #oldLogics: { [key: string]: Logic[] };

  #logicsToUpdate: Logic[];

  #logicsToCreate: Logic[];

  #matsToHandle: Materialization[];

  #matsToUpdate: Materialization[];

  #matsToCreate: Materialization[];

  #columnsToHandle: Column[];

  #oldColumns: { [key: string]: Column[] };

  #columnsToUpdate: Column[];

  #columnsToCreate: Column[];

  #matsToUpdate: Materialization[] = [];

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
    }
  ) {
    this.#lineageRepo = dependencies.lineageRepo;
    this.#createMaterialization = dependencies.createMaterialization;
    this.#createColumn = dependencies.createColumn;
    this.#materializationRepo = dependencies.materializationRepo;
    this.#columnRepo = dependencies.columnRepo;

    this.#dbConnection = dbConnection;
    this.#auth = auth;

    this.#organizationId = props.organizationId;
  }

  #buildUpdatedColumns = async (
    oldColumnProps: { id: string; name: string; lineageId: string },
    columnToHandle: Column
  ): Promise<Column> => {
    const createColumnResult = await this.#createColumn.execute(
        {
          ...props.columnToHandle.toDto(),
          id: props.oldMatProps.id,
          relationName: props.oldMatProps.relationName,
          writeToPersistence: false,
        },
        this.#auth,
        this.#dbConnection
      );
  
      if (!createMatResult.success) throw new Error(createMatResult.error);
      if (!createMatResult.value)
        throw new Error('Create Mat failed - Unknown error');
  
      return createMatResult.value;


  }

  #buildUpdatedMat = async (
    oldMatProps: { id: string; relationName: string; lineageId: string },
    matToHandle: Materialization
  ): Promise<Materialization> => {
    const createMatResult = await this.#createMaterialization.execute(
      {
        ...matToHandle.toDto(),
        id: oldMatProps.id,
        relationName: oldMatProps.relationName,
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

  merge = async (): Promise<void> => {
    // todo- needs to retrieve latest compeleted lineage
    const latestLineage = await this.#lineageRepo.findLatest(
      this.#dbConnection,
      this.#organizationId
    );

    if (!latestLineage) return;

    const latestMats = await this.#materializationRepo.findBy(
      { lineageIds: [latestLineage.id], organizationId: this.#organizationId },
      this.#dbConnection
    );

    const matsToUpdate: Materialization[] = [];
    const matsToCreate: Materialization[] = [];

    await Promise.all(
      this.#matsToHandle.map(async (matToHandle) => {
        const matchingMat = latestMats.find(
          (oldMat) => matToHandle.relationName === oldMat.relationName
        );

        if (matchingMat) {
          const updatedMat = await this.#buildUpdatedMat({
            oldMatProps: {
              lineageId: latestLineage.id,
              id: matchingMat.id,
              relationName: matchingMat.relationName,
            },
            matToHandle,
          });
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
            ).reduce(SnapshotMerger.#groupByMatId, {});

          if (!Object.keys(this.#oldLogics).length)
            this.#oldColumns = (
              await this.#columnRepo.findBy(
                {
                  lineageIds: [latestLineage.id],
                  organizationId: this.#organizationId,
                },
                this.#dbConnection
              )
            ).reduce(SnapshotMerger.#groupByMatId, {});
        } else {
          // todo - also logic, ...
          matsToCreate.push(matToHandle);
        }
      })
    );
  };
}
