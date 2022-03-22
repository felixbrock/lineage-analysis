import { Model } from "../entities/model";

export interface IModelRepo {
  findOne(id: string): Promise<Model | null>;
  all(): Promise<Model[]>;
  insertOne(model: Model): Promise<string>;
  deleteOne(id: string): Promise<string>;
}
