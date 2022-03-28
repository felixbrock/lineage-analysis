import { Model } from '../entities/model';
import { Logic } from '../value-types/logic';

export interface ModelQueryDto {
  location?: string;
  lineageId?: string;
}

export interface ModelUpdateDto {
  location?: string;
  logic?: Logic;
}

export interface IModelRepo {
  findOne(id: string): Promise<Model | null>;
  findBy(tableQueryDto: ModelQueryDto): Promise<Model[]>;
  all(): Promise<Model[]>;
  updateOne(id: string, updateDto: ModelUpdateDto): Promise<string>;
  insertOne(model: Model): Promise<string>;
  deleteOne(id: string): Promise<string>;
}
