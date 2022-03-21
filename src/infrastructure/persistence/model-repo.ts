import {
  DeleteResult,
  Document,
  FindCursor,
  InsertOneResult,
  ObjectId,
  UpdateResult,
} from 'mongodb';
import sanitize from 'mongo-sanitize';

import { connect, close, createClient } from './db/mongo-db';

interface ModelPersistence {
  _id: string;
  sql: string;
  statementReferences: [string, string][][];
}

interface ModelQueryFilter {
  content?: string;
  organizationId?: string;
  systemId?: string;
  modifiedOn?: { [key: string]: number };
}

interface ModelUpdateFilter {
  $set: { [key: string]: any };
  $push: { [key: string]: any };
}

const collectionName = 'model';

export default class ModelRepositoryImpl implements IModelRepository {
  public findOne = async (id: string): Promise<Model | null> => {
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

  public findBy = async (modelQueryDto: ModelQueryDto): Promise<Model[]> => {
    try {
      if (!Object.keys(modelQueryDto).length) return await this.all();

      const client = createClient();

      const db = await connect(client);
      const result: FindCursor = await db
        .collection(collectionName)
        .find(this.#buildFilter(sanitize(modelQueryDto)));
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

  #buildFilter = (modelQueryDto: ModelQueryDto): ModelQueryFilter => {
    const filter: { [key: string]: any } = {};

    if (modelQueryDto.content) filter.content = modelQueryDto.content;
    if (modelQueryDto.organizationId)
      filter.organizationId = modelQueryDto.organizationId;
    if (modelQueryDto.systemId) filter.systemId = modelQueryDto.systemId;

    const modifiedOnFilter: { [key: string]: number } = {};
    if (modelQueryDto.modifiedOnStart)
      modifiedOnFilter.$gte = modelQueryDto.modifiedOnStart;
    if (modelQueryDto.modifiedOnEnd)
      modifiedOnFilter.$lte = modelQueryDto.modifiedOnEnd;
    if (Object.keys(modifiedOnFilter).length)
      filter.modifiedOn = modifiedOnFilter;

    if (!modelQueryDto.alert || !Object.keys(modelQueryDto.alert).length)
      return filter;

    const alertCreatedOnFilter: { [key: string]: number } = {};
    if (modelQueryDto.alert.createdOnStart)
      alertCreatedOnFilter.$gte = modelQueryDto.alert.createdOnStart;
    if (modelQueryDto.alert.createdOnEnd)
      alertCreatedOnFilter.$lte = modelQueryDto.alert.createdOnEnd;
    if (Object.keys(alertCreatedOnFilter).length)
      filter['alerts.createdOn'] = alertCreatedOnFilter;

    return filter;
  };

  public all = async (): Promise<Model[]> => {
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

  public insertOne = async (model: Model): Promise<string> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: InsertOneResult<Document> = await db
        .collection(collectionName)
        .insertOne(this.#toPersistence(sanitize(model)));

      if (!result.acknowledged)
        throw new Error('Model creation failed. Insert not acknowledged');

      close(client);

      return result.insertedId.toHexString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  public updateOne = async (
    id: string,
    updateDto: ModelUpdateDto
  ): Promise<string> => {
    const client = createClient();
    try {
      const db = await connect(client);

      const result: Document | UpdateResult = await db
        .collection(collectionName)
        .updateOne(
          { _id: new ObjectId(sanitize(id)) },
          this.#buildUpdateFilter(sanitize(updateDto))
        );

      if (!result.acknowledged)
        throw new Error('Model update failed. Update not acknowledged');

      close(client);

      return result.upsertedId;
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  #buildUpdateFilter = (modelUpdateDto: ModelUpdateDto): ModelUpdateFilter => {
    const setFilter: { [key: string]: any } = {};
    const pushFilter: { [key: string]: any } = {};

    if (modelUpdateDto.content) setFilter.content = modelUpdateDto.content;
    if (modelUpdateDto.organizationId)
      setFilter.organizationId = modelUpdateDto.organizationId;
    if (modelUpdateDto.systemId) setFilter.systemId = modelUpdateDto.systemId;
    if (modelUpdateDto.modifiedOn)
      setFilter.modifiedOn = modelUpdateDto.modifiedOn;

    if (modelUpdateDto.alert)
      pushFilter.alerts = this.#alertToPersistence(modelUpdateDto.alert);

    return { $set: setFilter, $push: pushFilter };
  };

  public deleteOne = async (id: string): Promise<string> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: DeleteResult = await db
        .collection(collectionName)
        .deleteOne({ _id: new ObjectId(sanitize(id)) });

      if (!result.acknowledged)
        throw new Error('Model delete failed. Delete not acknowledged');

      close(client);

      return result.deletedCount.toString();
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  #toEntity = (modelProperties: ModelProperties): Model =>
    Model.create(modelProperties);

  #buildProperties = (model: ModelPersistence): ModelProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: model._id,
    content: model.content,
    organizationId: model.organizationId,
    systemId: model.systemId,
    modifiedOn: model.modifiedOn,
    alerts: model.alerts.map((alert) =>
      Alert.create({ createdOn: alert.createdOn })
    ),
  });

  #toPersistence = (model: Model): Document => ({
    _id: ObjectId.createFromHexString(model.id),
    content: model.content,
    organizationId: model.organizationId,
    systemId: model.systemId,
    modifiedOn: model.modifiedOn,
    alerts: model.alerts.map(
      (alert): AlertPersistence => this.#alertToPersistence(alert)
    ),
  });

  #alertToPersistence = (alert: Alert): AlertPersistence => ({
    createdOn: alert.createdOn,
  });
}
