import { Model } from '../value-types/model';

export interface ModelDto {
  sql: string;
  statementReferences: [string, string][][];
}

export const buildModelDto = (model: Model): ModelDto => ({
  sql: model.sql,
  statementReferences: model.statementReferences,
});
