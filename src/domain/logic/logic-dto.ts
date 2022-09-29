import { Logic } from "../entities/logic";

export interface LogicDto {
  id: string;
  relationName: string;
  sql: string;
  dependentOn: any[];
  parsedLogic: string;
  // todo - Should be turned into a value-type and returned as a dto?
  statementRefs: any;
  lineageId: string;
  organizationId: string;
}

export const buildLogicDto = (logic: Logic): LogicDto => ({
  id: logic.id,
  relationName: logic.relationName,
  sql: logic.sql,
  dependentOn: logic.dependentOn,
  parsedLogic: logic.parsedLogic,
  statementRefs: logic.statementRefs,
  lineageId: logic.lineageId,
  organizationId: logic.organizationId
});
