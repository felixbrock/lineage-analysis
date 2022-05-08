import { Logic } from '../entities/logic';

export interface LogicDto {
  id: string;
  dbtModelId: string;
  sql: string;
  parsedLogic: string;
  // todo - Should be turned into a value-type and returned as a dto?
  statementRefs: any[];
  lineageId: string;
}

export const buildLogicDto = (logic: Logic): LogicDto => ({
  id: logic.id,
  dbtModelId: logic.dbtModelId,
  sql: logic.sql,
  parsedLogic: logic.parsedLogic,
  statementRefs: logic.statementRefs,
  lineageId: logic.lineageId,
});
