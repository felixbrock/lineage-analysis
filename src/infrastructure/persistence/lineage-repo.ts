import fs from 'fs';
import { Lineage, LineageProperties } from '../../domain/value-types/dependency';

interface LineagePersistence {
  _id: string;
  lineage: { [key: string]: any }[];
}

const collectionName = 'lineage';

export default class LineageRepositoryImpl {
  public findOne = async (id: string): Promise<Lineage | null> => {
    const result = await fs.readFileSync('db.json', 'utf8');

    const obj = JSON.parse(result);
    return this.#toEntity(this.#buildProperties(obj[id]));
    // const client = createClient();
    // try {
    //   const db = await connect(client);
    //   const result: any = await db
    //     .collection(collectionName)
    //     .findOne({ _id: new ObjectId(sanitize(id)) });
    //   close(client);
    //   if (!result) return null;
    //   return this.#toEntity(this.#buildProperties(result));
    // } catch (error: unknown) {
    //   if (typeof error === 'string') return Promise.reject(error);
    //   if (error instanceof Error) return Promise.reject(error.message);
    //   return Promise.reject(new Error('Unknown error occured'));
    // }
  };

  public insertOne = async (lineage: Lineage): Promise<string> => {
    const result = fs.readFileSync('db.json', 'utf8');

    const obj = JSON.parse(result);
    const newLineage = JSON.stringify(this.#toPersistence(lineage)); 
    obj[lineage.id] = newLineage
    fs.writeFileSync('db.json', obj, 'utf8');

    return newLineage

    // const client = createClient();
    // try {
    //   const db = await connect(client);
    //   const result: InsertOneResult<Document> = await db
    //     .collection(collectionName)
    //     .insertOne(this.#toPersistence(sanitize(lineage)));

    //   if (!result.acknowledged)
    //     throw new Error('Lineage creation failed. Insert not acknowledged');

    //   close(client);

    //   return result.insertedId.toHexString();
    // } catch (error: unknown) {
    //   if (typeof error === 'string') return Promise.reject(error);
    //   if (error instanceof Error) return Promise.reject(error.message);
    //   return Promise.reject(new Error('Unknown error occured'));
    // }
  };

  #toEntity = (lineageProperties: LineageProperties): Lineage =>
    Lineage.create(lineageProperties);

  #buildProperties = (lineage: LineagePersistence): LineageProperties => ({
    // eslint-disable-next-line no-underscore-dangle
    id: lineage._id,
    lineage: lineage.lineage,
  });

  #toPersistence = (lineage: Lineage): LineagePersistence => ({
    _id: lineage.id,
    lineage: lineage.lineage,
  });
}
