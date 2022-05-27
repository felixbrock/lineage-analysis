import {
  Db,
  DeleteResult,
  Document,
  FindCursor,
  InsertManyResult,
  InsertOneResult,
  ObjectId,
} from 'mongodb';
import sanitize from 'mongo-sanitize';

import {
  IMaterializationRepo,
  MaterializationQueryDto,
} from '../../domain/materialization/i-materialization-repo';
import {
  MaterializationType,
  Materialization,
  MaterializationProperties,
} from '../../domain/entities/materialization';

interface MaterializationPersistence {
  _id: ObjectId;
  materializationType: MaterializationType;
  dbtModelId: string;
  name: string;
  schemaName: string;
  databaseName: string;
  logicId: string;
  lineageId: string;
}

interface MaterializationQueryFilter {
  materializationType?: MaterializationType;
  dbtModelId?: RegExp;
  name?: RegExp | { [key: string]: RegExp[] };
  schemaName?: RegExp;
  databaseName?: RegExp;
  logicId?: string;
  lineageId: string;
}

const collectionName = 'materialization';

export default class MaterializationRepo implements IMaterializationRepo {
  findOne = async (id: string, dbConnection: Db): Promise<Materialization | null> => {
    
    try {
      
      const result: any = await dbConnection
        .collection(collectionName)
        .findOne({ _id: new ObjectId(sanitize(id)) });

      

      if (!result) return null;

      return this.#toEntity(this.#buildProperties(result));
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  findBy = async (
    materializationQueryDto: MaterializationQueryDto, dbConnection: Db
  ): Promise<Materialization[]> => {
    try {
      if (!Object.keys(materializationQueryDto).length) return await this.all(dbConnection);

      

      
      const result: FindCursor = await dbConnection
        .collection(collectionName)
        .find(this.#buildFilter(sanitize(materializationQueryDto)));
      const results = await result.toArray();

      

      if (!results || !results.length) return [];

      return results.map((element: any) =>
        this.#toEntity(this.#buildProperties(element))
      );
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  #buildFilter = (
    materializationQueryDto: MaterializationQueryDto
  ): MaterializationQueryFilter => {
    const filter: MaterializationQueryFilter = {
      lineageId: materializationQueryDto.lineageId,
    };

    if (materializationQueryDto.materializationType)
      filter.materializationType = materializationQueryDto.materializationType;
    if (materializationQueryDto.dbtModelId)
      filter.dbtModelId = new RegExp(`^${materializationQueryDto.dbtModelId}$`, 'i');

    if (
      typeof materializationQueryDto.name === 'string' &&
      materializationQueryDto.name
    )
      filter.name = new RegExp(`^${materializationQueryDto.name}$`, 'i');
    if (materializationQueryDto.name instanceof Array)
      filter.name = {
        $in: materializationQueryDto.name.map(
          (element) => new RegExp(`^${element}$`, 'i')
        ),
      };

    if (materializationQueryDto.schemaName)
      filter.schemaName = new RegExp(`^${materializationQueryDto.schemaName}$`, 'i');
    if (materializationQueryDto.databaseName)
      filter.databaseName = new RegExp(
        `^${materializationQueryDto.databaseName}$`,
        'i'
      );
    if (materializationQueryDto.logicId)
      filter.logicId = materializationQueryDto.logicId;

    return filter;
  };

  all = async (dbConnection: Db): Promise<Materialization[]> => {
    
    try {
      
      const result: FindCursor = await dbConnection.collection(collectionName).find();
      const results = await result.toArray();

      

      if (!results || !results.length) return [];

      return results.map((element: any) =>
        this.#toEntity(this.#buildProperties(element))
      );
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  insertOne = async (materialization: Materialization, dbConnection: Db): Promise<string> => {
    
    try {
      
      const result: InsertOneResult<Document> = await dbConnection
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(materialization)));

      if (!result.acknowledged)
        throw new Error(
          'Materialization creation failed. Insert not acknowledged'
        );

      

      return result.insertedId.toHexString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  insertMany = async (
    materializations: Materialization[], dbConnection: Db
  ): Promise<string[]> => {
    try {
      
      const result: InsertManyResult<Document> = await dbConnection
        .collection(collectionName)
        .insertMany(
          materializations.map((element) =>
            this.#toPersistence(sanitize(element))
          )
        );

      if (!result.acknowledged)
        throw new Error('Logic creations failed. Inserts not acknowledged');

      return Object.keys(result.insertedIds).map((key) =>
        result.insertedIds[parseInt(key, 10)].toHexString()
      );
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  deleteOne = async (id: string, dbConnection: Db): Promise<string> => {
    
    try {
      
      const result: DeleteResult = await dbConnection
        .collection(collectionName)
        .deleteOne({ _id: new ObjectId(sanitize(id)) });

      if (!result.acknowledged)
        throw new Error(
          'Materialization delete failed. Delete not acknowledged'
        );

      

      return result.deletedCount.toString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  #toEntity = (
    materializationProperties: MaterializationProperties
  ): Materialization => Materialization.create(materializationProperties);

  #buildProperties = (
    materialization: MaterializationPersistence
  ): MaterializationProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: materialization._id.toHexString(),
    materializationType: materialization.materializationType,
    dbtModelId: materialization.dbtModelId,
    name: materialization.name,
    schemaName: materialization.schemaName,
    databaseName: materialization.databaseName,
    logicId: materialization.logicId,
    lineageId: materialization.lineageId,
  });

  #toPersistence = (materialization: Materialization): Document => ({
    _id: ObjectId.createFromHexString(materialization.id),
    materializationType: materialization.materializationType,
    dbtModelId: materialization.dbtModelId,
    name: materialization.name,
    schemaName: materialization.schemaName,
    databaseName: materialization.databaseName,
    logicId: materialization.logicId,
    lineageId: materialization.lineageId,
  });
}
