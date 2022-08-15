// todo clean architecture violation
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import {
  CatalogModelData,
  Logic,
  MaterializationDefinition,
} from '../entities/logic';
import { ILogicRepo } from './i-logic-repo';
import { ReadLogics } from './read-logics';
import { DbConnection } from '../services/i-db';

export interface CreateLogicRequestDto {
  dbtModelId: string;
  modelName: string;
  sql: string;
  dependentOn: MaterializationDefinition[];
  parsedLogic: string;
  lineageId: string;
  writeToPersistence: boolean;
  targetOrganizationId: string;
  catalogFile:string;
}

export interface CreateLogicAuthDto {
  isSystemInternal: boolean;
  callerOrganizationId: string;
}

export type CreateLogicResponse = Result<Logic>;

export class CreateLogic
  implements
    IUseCase<
      CreateLogicRequestDto,
      CreateLogicResponse,
      CreateLogicAuthDto,
      DbConnection
    >
{
  readonly #readLogics: ReadLogics;

  readonly #logicRepo: ILogicRepo;

  #dbConnection: DbConnection;

  constructor(readLogics: ReadLogics, logicRepo: ILogicRepo) {
    this.#readLogics = readLogics;
    this.#logicRepo = logicRepo;
  }

  #getTablesAndCols = (catalogFile:string): CatalogModelData[] => {
    const data = catalogFile;

    const catalog = JSON.parse(data);
    const catalogNodes = catalog.nodes;

    const result: CatalogModelData[] = [];

    Object.entries(catalogNodes).forEach((entry) => {
      const [modelName, body]: [string, any] = entry;
      const { metadata, columns } = body;

      const { name } = metadata;
      const columnNames = Object.keys(columns);

      const modelData: CatalogModelData = {
        modelName,
        materializationName: name,
        columnNames,
      };

      result.push(modelData);
    });

    return result;
  };

  async execute(
    request: CreateLogicRequestDto,
    auth: CreateLogicAuthDto,
    dbConnection: DbConnection
  ): Promise<CreateLogicResponse> {
    try {
      if (!auth.isSystemInternal) throw new Error('Unauthorized');

      this.#dbConnection = dbConnection;

      const catalog = this.#getTablesAndCols(request.catalogFile);

      const logic = Logic.create({
        id: new ObjectId().toHexString(),
        dbtModelId: request.dbtModelId,
        modelName: request.modelName,
        sql: request.sql,
        dependentOn: request.dependentOn,
        parsedLogic: request.parsedLogic,
        lineageId: request.lineageId,
        organizationId: auth.callerOrganizationId,
        catalog,
      });

      const readLogicsResult = await this.#readLogics.execute(
        {
          dbtModelId: request.dbtModelId,
          lineageId: request.lineageId,
          targetOrganizationId: request.targetOrganizationId,
        },
        {
          callerOrganizationId: auth.callerOrganizationId,
          isSystemInternal: auth.isSystemInternal,
        },
        this.#dbConnection
      );

      if (!readLogicsResult.success) throw new Error(readLogicsResult.error);
      if (!readLogicsResult.value) throw new Error('Reading logics failed');
      if (readLogicsResult.value.length)
        throw new ReferenceError('Logic to be created already exists');

      if (request.writeToPersistence)
        await this.#logicRepo.insertOne(logic, dbConnection);

      return Result.ok(logic);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
