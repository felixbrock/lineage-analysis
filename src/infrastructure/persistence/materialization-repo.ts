import {
  DeleteResult,
  Document,
  FindCursor,
  InsertManyResult,
  InsertOneResult,
  ObjectId,
} from 'mongodb';
import sanitize from 'mongo-sanitize';

import { connect, close, createClient } from './db/mongo-db';
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
  dbtModelId?: string;
  name?: string | { [key: string]: string[] };
  schemaName?: string;
  databaseName?: string;
  logicId?: string;
  lineageId: string;
}

const collectionName = 'materialization';

export default class MaterializationRepo implements IMaterializationRepo {
  findOne = async (id: string): Promise<Materialization | null> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: any = await db
        .collection(collectionName)
        .findOne({ _id: new ObjectId(sanitize(id)) });

      close(client);

      if (!result) return null;

      return this.#toEntity(this.#buildProperties(result));
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  findBy = async (
    materializationQueryDto: MaterializationQueryDto
  ): Promise<Materialization[]> => {
    try {
      if (!Object.keys(materializationQueryDto).length) return await this.all();

      const client = createClient();

      const db = await connect(client);
      const result: FindCursor = await db
        .collection(collectionName)
        .find(this.#buildFilter(sanitize(materializationQueryDto)));
      const results = await result.toArray();

      close(client);

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
      filter.dbtModelId = materializationQueryDto.dbtModelId;

    if (
      typeof materializationQueryDto.name === 'string' &&
      materializationQueryDto.name
    )
      filter.name = materializationQueryDto.name;
    if (materializationQueryDto.name instanceof Array)
      filter.name = { $in: materializationQueryDto.name };

    if (materializationQueryDto.schemaName)
      filter.schemaName = materializationQueryDto.schemaName;
    if (materializationQueryDto.databaseName)
      filter.databaseName = materializationQueryDto.databaseName;
    if (materializationQueryDto.logicId)
      filter.logicId = materializationQueryDto.logicId;

    return filter;
  };

  all = async (): Promise<Materialization[]> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: FindCursor = await db.collection(collectionName).find();
      const results = await result.toArray();

      close(client);

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

  insertOne = async (materialization: Materialization): Promise<string> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: InsertOneResult<Document> = await db
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(materialization)));

      if (!result.acknowledged)
        throw new Error(
          'Materialization creation failed. Insert not acknowledged'
        );

      close(client);

      return result.insertedId.toHexString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  insertMany = async (
    materializations: Materialization[]
  ): Promise<string[]> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: InsertManyResult<Document> = await db
        .collection(collectionName)
        .insertMany(
          materializations.map((element) =>
            this.#toPersistence(sanitize(element))
          )
        );

      if (!result.acknowledged)
        throw new Error('Logic creations failed. Inserts not acknowledged');

      close(client);

      return Object.keys(result.insertedIds).map((key) =>
        result.insertedIds[parseInt(key, 10)].toHexString()
      );
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  deleteOne = async (id: string): Promise<string> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: DeleteResult = await db
        .collection(collectionName)
        .deleteOne({ _id: new ObjectId(sanitize(id)) });

      if (!result.acknowledged)
        throw new Error(
          'Materialization delete failed. Delete not acknowledged'
        );

      close(client);

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
