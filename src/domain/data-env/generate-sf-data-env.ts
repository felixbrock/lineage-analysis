// todo clean architecture violation
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import BaseAuth from '../services/base-auth';
import { DataEnvProps } from './data-env';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import BaseGetSfDataEnv, { ColumnRepresentation } from './base-get-sf-data-env';
import { Materialization } from '../entities/materialization';
import { Column } from '../entities/column';
import { Logic } from '../entities/logic';
import { QuerySnowflake } from '../snowflake-api/query-snowflake';
import { CreateMaterialization } from '../materialization/create-materialization';
import { CreateColumn } from '../column/create-column';
import { CreateLogic } from '../logic/create-logic';
import { ParseSQL } from '../sql-parser-api/parse-sql';

export type GenerateSfDataEnvRequestDto = undefined;

export interface GenerateSfDataEnvAuthDto
  extends Omit<BaseAuth, 'callerOrgId'> {
  callerOrgId: string;
}

export type GenerateSfDataEnvResponse = Result<DataEnvProps>;

export class GenerateSfDataEnv
  extends BaseGetSfDataEnv
  implements
    IUseCase<
      GenerateSfDataEnvRequestDto,
      GenerateSfDataEnvResponse,
      GenerateSfDataEnvAuthDto,
      IConnectionPool
    >
{
  readonly #mats: Materialization[] = [];

  readonly #cols: Column[] = [];

  readonly #logics: Logic[] = [];

  constructor(
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
  }

  #generateDbResources = async (dbName: string): Promise<void> => {
    const binds = ['information_schema'];
    const whereCondition = `table_schema not ilike ?`;
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

        const resource = await this.generateDWResource(
          {
            matRepresentation: el,
            logicRepresentation,
            columnRepresentations:
              colRepresentationsByRelationName[el.relationName],
            relationName: el.relationName,
          },
          options
        );

        this.#mats.push(resource.matToCreate);
        this.#cols.push(...resource.colsToCreate);
        this.#logics.push(resource.logicToCreate);
      })
    );
  };

  /* Runs through snowflake and creates objects like logic, materializations and columns */
  async execute(
    req: GenerateSfDataEnvRequestDto,
    auth: GenerateSfDataEnvAuthDto,
    connPool: IConnectionPool
  ): Promise<GenerateSfDataEnvResponse> {
    try {
      this.connPool = connPool;
      this.auth = auth;

      const dbRepresentations = await this.getDbRepresentations(connPool, auth);

      await Promise.all(
        dbRepresentations.map(async (el) => {
          await this.#generateDbResources(el.name);
        })
      );

      const dependenciesToCreate = await this.generateDependencies(this.#mats);

      return Result.ok({
        dataEnv: {
          matsToCreate: this.#mats,
          columnsToCreate: this.#cols,
          logicsToCreate: this.#logics,
          dependenciesToCreate,
          deleteAllOldDependencies: false,
          columnsToReplace: [],
          columnToDeleteRefs: [],
          logicsToReplace: [],
          logicToDeleteRefs: [],
          matsToReplace: [],
          matToDeleteRefs: [],
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
