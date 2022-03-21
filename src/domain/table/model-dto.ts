import { Model, StatementReference } from '../entities/model';

export interface ModelDto {
  sql: string;
  statementReferences: StatementReference[][];
}

export const buildModelDto = (model: Model): ModelDto => ({
  sql: model.sql,
  statementReferences: model.statementReferences,
});
