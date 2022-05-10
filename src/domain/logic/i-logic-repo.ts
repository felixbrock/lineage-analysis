import { Logic } from '../entities/logic';

export interface LogicQueryDto {
  dbtModelId?: string;
  lineageId: string;
}

export interface ILogicRepo {
  findOne(id: string): Promise<Logic | null>;
  findBy(materializationQueryDto: LogicQueryDto): Promise<Logic[]>;
  all(): Promise<Logic[]>;
  insertOne(logic: Logic): Promise<string>;
  insertMany(logics: Logic[]): Promise<string[]>;
  deleteOne(id: string): Promise<string>;
}
