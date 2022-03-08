import { Model } from '../value-types/model';

export interface ModelDto {
  sql: string;
  statementDependencies: [string, string][][];
}

export const buildModelDto = (model: Model): ModelDto => ({
  sql: model.sql,
  statementDependencies: model.statementDependencies,
});
