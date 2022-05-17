// todo clean architecture violation
import fs from 'fs'; // 'todo - dependency rule violation'
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

export interface CreateLogicRequestDto {
  dbtModelId: string;
  modelName: string;
  sql: string;
  dependentOn: MaterializationDefinition[];
  parsedLogic: string;
  lineageId: string;
  writeToPersistence: boolean;
}

export interface CreateLogicAuthDto {
  organizationId: string;
}

export type CreateLogicResponse = Result<Logic>;

export class CreateLogic
  implements
    IUseCase<CreateLogicRequestDto, CreateLogicResponse, CreateLogicAuthDto>
{
  readonly #readLogics: ReadLogics;

  readonly #logicRepo: ILogicRepo;

  constructor(readLogics: ReadLogics, logicRepo: ILogicRepo) {
    this.#readLogics = readLogics;
    this.#logicRepo = logicRepo;
  }

  #getTablesAndCols = (): CatalogModelData[] => {
    const data = fs.readFileSync(
      // `C:/Users/felix-pc/Documents/Repositories/lineage-analysis/test/use-cases/dbt/catalog/web-samples/sample-1-no-v_date_stg.json`,
      `C:/Users/nasir/OneDrive/Desktop/lineage-analysis/test/use-cases/dbt/catalog/catalog.json`,
      'utf-8'
    );
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
    auth: CreateLogicAuthDto
  ): Promise<CreateLogicResponse> {
    try {
      const catalog = this.#getTablesAndCols();

      const logic = Logic.create({
        id: new ObjectId().toHexString(),
        dbtModelId: request.dbtModelId,
        modelName: request.modelName,
        sql: request.sql,
        dependentOn: request.dependentOn,
        parsedLogic: request.parsedLogic,
        lineageId: request.lineageId,
        catalog,
      });

      const readLogicsResult = await this.#readLogics.execute(
        {
          dbtModelId: request.dbtModelId,
          lineageId: request.lineageId,
        },
        { organizationId: auth.organizationId }
      );

      if (!readLogicsResult.success) throw new Error(readLogicsResult.error);
      if (!readLogicsResult.value) throw new Error('Reading logics failed');
      if (readLogicsResult.value.length)
        throw new ReferenceError('Logic to be created already exists');

      if (request.writeToPersistence) await this.#logicRepo.insertOne(logic);

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      return Result.ok(logic);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
