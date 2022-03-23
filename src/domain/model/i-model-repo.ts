import { Model } from '../entities/model';

export interface ModelQueryDto {
  location?: string;
}

export interface IModelRepo {
  findOne(id: string): Promise<Model | null>;
  findBy(tableQueryDto: ModelQueryDto): Promise<Model[]>;
  all(): Promise<Model[]>;
  insertOne(model: Model): Promise<string>;
  deleteOne(id: string): Promise<string>;
}
