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
import {
  IModelRepo,
  ModelQueryDto,
  ModelUpdateDto,
} from '../../domain/model/i-model-repo';
import { Model, ModelPrototype } from '../../domain/entities/model';

interface LogicPersistence {
  parsedLogic: string;
  statementReferences: [string, string][][];
}

interface ModelPersistence {
  _id: ObjectId;
  location: string;
  logic: LogicPersistence;
  lineageId: string;
}

interface ModelQueryFilter {
  location?: string;
  lineageId: string;
}

interface ModelUpdateFilter {
  $set: { [key: string]: any };
}

const collectionName = 'model';

export default class ModelRepo implements IModelRepo {
  findOne = async (id: string): Promise<Model | null> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: any = await db
        .collection(collectionName)
        .findOne({ _id: new ObjectId(sanitize(id)) });

      close(client);

      if (!result) return null;

      return this.#toEntity(this.#buildPrototype(result));
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  findBy = async (modelQueryDto: ModelQueryDto): Promise<Model[]> => {
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
        this.#toEntity(this.#buildPrototype(element))
      );
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  #buildFilter = (modelQueryDto: ModelQueryDto): ModelQueryFilter => {
    const filter: ModelQueryFilter = {lineageId: modelQueryDto.lineageId};

    if (modelQueryDto.location) filter.location = modelQueryDto.location;

    return filter;
  };

  all = async (): Promise<Model[]> => {
    const client = createClient();
    try {
      const db = await connect(client);
      const result: FindCursor = await db.collection(collectionName).find();
      const results = await result.toArray();

      close(client);

      if (!results || !results.length) return [];

      return results.map((element: any) =>
        this.#toEntity(this.#buildPrototype(element))
      );
    } catch (error: unknown) {
      if (typeof error === 'string') return Promise.reject(error);
      if (error instanceof Error) return Promise.reject(error.message);
      return Promise.reject(new Error('Unknown error occured'));
    }
  };

  updateOne = async (
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

    if (modelUpdateDto.location) setFilter.location = modelUpdateDto.location;
    if (modelUpdateDto.logic) setFilter.logic = modelUpdateDto.logic;

    return { $set: setFilter };
  };

  insertOne = async (model: Model): Promise<string> => {
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

  #toEntity = (modelPrototype: ModelPrototype): Model =>
    Model.create(modelPrototype);

  #buildPrototype = (model: ModelPersistence): ModelPrototype => ({
    // eslint-disable-next-line no-underscore-dangle
    id: model._id.toHexString(),
    location: model.location,
    // todo - Doesnt make sense to generate logic object from scratch if it exists in persistence
    parsedLogic: model.logic.parsedLogic,
    lineageId: model.lineageId,
  });

  #toPersistence = (model: Model): Document => ({
    _id: ObjectId.createFromHexString(model.id),
    location: model.location,
    lineageId: model.lineageId,
    logic: {
      parsedLogic: model.logic.parsedLogic,
      statementReferences: model.logic.statementReferences,
    },
  });
}
