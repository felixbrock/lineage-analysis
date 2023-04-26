import { Document } from 'mongodb';
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
  Binds,
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
import { IDb, IDbConnection } from '../services/i-db';

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
      IDb
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
    oldCols: Column[],
    oldMatId: string
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
              materializationId: oldMatId,
            },
            columnToHandle
          );

          columnsToReplace.push(updatedColumn);
        } else
          columnsToCreate.push(
            Column.build({
              ...columnToHandle.toDto(),
              materializationId: oldMatId,
            })
          );
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

  #getDataEnvDiff = (values: Document[]): DataEnvDiff => {
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
      (accumulation: DataEnvDiff, el: Document): DataEnvDiff => {
        const localAcc = accumulation;

        const {
          mat_deleted_id: matDeletedId,
          mat_added_relation_name: matAddedRelationName,
          altered,
        } = el;

        let matDeletedIdValue = matDeletedId;
        if (!matDeletedId) matDeletedIdValue = null;
        
        let matAddedRelationNameValue = matAddedRelationName;
        if (!matAddedRelationName) matAddedRelationNameValue = null;

        const alteredValue = JSON.parse(altered);

        if (typeof alteredValue !== 'boolean')
          throw new Error('Unexpected altered value');
        if (!isOptionalOfType<string>(matDeletedIdValue, 'string'))
          throw new Error('Unexpected deletedMatId value');
        if (!isOptionalOfType<string>(matAddedRelationNameValue, 'string'))
          throw new Error('Unexpected addedMatId value');

        if (alteredValue) {
          if (
            typeof matAddedRelationNameValue !== 'string' ||
            typeof matDeletedIdValue !== 'string'
          )
            throw new Error('Unexpected altered column input');
          localAcc.matModifiedDiffs.push({
            oldMatId: matDeletedIdValue,
            relationName: matAddedRelationNameValue,
          });
        } else if (matAddedRelationNameValue)
          localAcc.matAddedRelationNames.push(matAddedRelationNameValue);
        else if (matDeletedIdValue) localAcc.matDeletedIds.push(matDeletedIdValue);
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
      .map((el) => `'${el}'`)
      .join(', ')}))`;
    const binds: Binds = [];
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
    if (!this.auth || !this.dbConnection)
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
      this.dbConnection
    );

    const oldCols = await this.#columnRepo.findBy(
      { materializationIds: ids },
      this.auth,
      this.dbConnection
    );

    const oldColsByMatId: { [key: string]: Column[] } = oldCols.reduce(
      this.#groupByMatId,
      {}
    );

    const oldLogics = await this.#logicRepo.findBy(
      { relationNames },
      this.auth,
      this.dbConnection
    );

    const whereCondition = `array_contains(concat(table_catalog, '.', table_schema, '.', table_name)::variant, array_construct(${relationNames
      .map((el) => `'${el}'`)
      .join(', ')}))`;
    const binds: Binds = [];
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
            oldColsByMatId[matchingMat.id],
            updatedMat.id
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
    if (!this.auth || !this.dbConnection)
      throw new Error('auth or connPool missing');

    if (!props.matToDeleteIds.length) return;

    const mats =
      props.matsToDelete ||
      (await this.#materializationRepo.findBy(
        { ids: props.matToDeleteIds },
        this.auth,
        this.dbConnection
      ));

    if (!mats.length) throw new Error('Desired mats not found');

    const cols = await this.#columnRepo.findBy(
      { materializationIds: props.matToDeleteIds },
      this.auth,
      this.dbConnection
    );

    const logics = await this.#logicRepo.findBy(
      { relationNames: mats.map((el) => el.relationName) },
      this.auth,
      this.dbConnection
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
    lastLineageCompletedAt: string,
    dbConnection: IDbConnection,
    auth: UpdateSfDataEnvAuthDto
  ): Promise<void> => {
    if (!this.auth || !this.dbConnection || !this.connPool)
      throw new Error('Missing auth or dbConnection or connPool');

    const tempCollectionName = `temp_collection_${auth.callerOrgId}_${dbName}`;
    const matsCollectionName = `materializations_${auth.callerOrgId}`;
    const fullJoinName = `full_join_${auth.callerOrgId}_${dbName}`;

    if ((await dbConnection
        .listCollections({ name: tempCollectionName })
        .toArray()).length > 0) {
      await dbConnection.collection(tempCollectionName).deleteMany({})
      .then()
      .catch((err) => {
        throw err;
      });
    } else {
      await dbConnection.createCollection(tempCollectionName)
      .then()
      .catch((err) => {
        throw err;
      });
    }

    if ((await dbConnection
        .listCollections({ name: fullJoinName })
        .toArray()).length > 0) {
      await dbConnection.collection(fullJoinName).deleteMany({})
      .then()
      .catch((err) => {
        throw err;
      });
    } else {
      await dbConnection.createCollection(fullJoinName)
      .then()
      .catch((err) => {
        throw err;
      });
    }

    const queryText = `select table_catalog, table_schema, table_name, last_altered from "${dbName}".information_schema.tables;`;
    const binds = [dbName];

    const queryResult = await this.querySnowflake.execute(
      { queryText, binds },
      this.auth,
      this.connPool
    );

    if (!queryResult.success) throw new Error(queryResult.error);
    if (!queryResult.value)
      throw new Error('Query result is missing value field');

    const docs = queryResult.value.map((row) => {
      const doc = {
        table_name: row.TABLE_NAME,
        table_schema: row.TABLE_SCHEMA,
        table_catalog: row.TABLE_CATALOG,
        last_altered: row.LAST_ALTERED instanceof Date ? row.LAST_ALTERED.toISOString() : null,
        concatted_name: `${row.TABLE_CATALOG}.${row.TABLE_SCHEMA}.${row.TABLE_NAME}`
      };

      return doc;
    });

    await dbConnection.collection(tempCollectionName).insertMany(docs);

    const leftJoin = [
      {
        $lookup: {
          from: tempCollectionName,
          localField: "relation_name",
          foreignField: "concatted_name",
          as: "leftJoin"
        }
      },
      {
        $unwind: {
          path: "$leftJoin",
          preserveNullAndEmptyArrays: true
        }
      },
      {
        $replaceRoot: {
          newRoot: {
            $mergeObjects: ["$leftJoin", "$$ROOT"]
          }
        }
      },
      {
        $merge: {
          into: fullJoinName
        } 
      },
    ];

    await dbConnection.collection(matsCollectionName).aggregate(leftJoin).toArray();

    const rightJoin = [
      {
        $lookup: {
          from: matsCollectionName,
          localField: "concatted_name",
          foreignField: "relation_name",
          as: "rightJoin"
        }
      },
      {
        $unwind: {
          path: "$rightJoin",
          preserveNullAndEmptyArrays: true
        }
      },
      {
        $replaceRoot: {
          newRoot: {
            $mergeObjects: ["$rightJoin", "$$ROOT"]
          }
        }
      },
      {
        $merge: {
          into: fullJoinName  
        }
      }
    ];

  await dbConnection.collection(tempCollectionName).aggregate(rightJoin).toArray();

    const pipeline = [
      {
        $match: {
          $and: [
            {
              $and: [
                { "database_name": { $in: [dbName, null] }},
                { "table_catalog": { $in: [dbName, null] }}
              ] 
            },
            {
              $or: [
                {
                  $or: [
                    { "table_name": null },
                    {
                      $and: [
                        { "relation_name": null },
                        { "table_schema": { $ne: "INFORMATION_SCHEMA" }}
                      ]
                    }
                  ]
                },
                {
                  $expr: {
                    $gt: [
                      {
                        $divide: [
                          {
                            $subtract: [
                              { $dateFromString: { dateString: lastLineageCompletedAt } },
                              { $dateFromString: { dateString: "$last_altered" } }
                            ]
                          },
                          60000
                        ]  
                      },
                      0
                    ]
                  }
                }
              ]
            }
          ]
        }  
      },
      {
        $project: {
          "mat_deleted_id": "$id",
          "mat_added_relation_name": "$concatted_name",
          "altered": {
            $cond: {
              if: { $and: [ { $ne: [ "$table_name", null ] }, { $ne: [ "$relation_name", null ] } ] },
              then: true,
              else: false
            }
          }
        }
      },
      {
        $group: {
          _id: "$mat_deleted_id",
          mat_deleted_id: { $first: "$mat_deleted_id" },
          mat_added_relation_name: { $first: "$mat_added_relation_name" },
          altered: { $first: "$altered" }
        }
      },
      {
        $project: {
          _id: 0,
          mat_deleted_id: 1,
          mat_added_relation_name: 1,
          altered: 1
        }
      }
    ];

    const result = await dbConnection
    .collection(fullJoinName).aggregate(pipeline).toArray();

    await dbConnection.collection(tempCollectionName).drop()
      .then()
      .catch((err) => {
        throw err;
      });

    await dbConnection.collection(fullJoinName).drop()
      .then()
      .catch((err) => {
        throw err;
      });

    const dataEnvDiff = this.#getDataEnvDiff(result);

    await this.#addResourcesToAdd(dataEnvDiff.matAddedRelationNames, dbName);
    await this.#addResourcesToDelete({
      matToDeleteIds: dataEnvDiff.matDeletedIds,
    });
    await this.#addResourcesToModify(dataEnvDiff.matModifiedDiffs, dbName);
  };

  #addRemovedDbToDelete = async (dbName: string): Promise<void> => {
    if (!this.auth || !this.dbConnection)
      throw new Error('Missing auth or connPool');

    const matsToDelete = await this.#materializationRepo.findBy(
      { databaseName: dbName },
      this.auth,
      this.dbConnection
    );

    const matIds = matsToDelete.map((el) => el.id);

    await this.#addResourcesToDelete({ matToDeleteIds: matIds, matsToDelete });
  };

  /* Checks Snowflake resources for changes and returns partial data env to merge with existing snapshot */
  async execute(
    req: UpdateSfDataEnvRequestDto,
    auth: UpdateSfDataEnvAuthDto,
    db: IDb
  ): Promise<UpdateSfDataEnvResponse> {
    try {
      this.auth = auth;
      this.connPool = db.sfConnPool;
      this.dbConnection = db.mongoConn;

      const dbRepresentations = await this.getDbRepresentations(this.connPool, this.auth);

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
            req.latestCompletedLineage.completedAt,
            db.mongoConn,
            auth
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
