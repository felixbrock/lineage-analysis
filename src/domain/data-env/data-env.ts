import { Column } from "../entities/column";
import { Logic, ModelRepresentation} from "../entities/logic";
import { Materialization } from "../entities/materialization";

export interface MatToDeleteRef {
  id: string;
  name: string;
  schemaName: string;
  dbName: string;
}

export interface ColToDeleteRef {
  id: string;
  name: string;
  relationName: string;
  matId: string;
}

export interface LogicToDeleteRef {
  id: string;
  relationName: string;
}


interface DataEnvDtoMat {
  dbName: string;
  schemaName:string;
  name: string;
}

interface DataEnvDtoCol {
  relationName: string;
  name: string
}

interface DataEnvDtoLogic {
  relationName: string;
}

export interface DataEnvDto {
  matToCreateRelationNames: DataEnvDtoMat[];
  matToReplaceRelationNames: DataEnvDtoMat[];
  matToDeleteRelationNames: DataEnvDtoMat[];
  colToCreateRelationNames: DataEnvDtoCol[];
  colToReplaceRelationNames: DataEnvDtoCol[];
  colToDeleteRelationNames: DataEnvDtoCol[];
  logicToCreateRelationNames: DataEnvDtoLogic[];
  logicToReplaceRelationNames: DataEnvDtoLogic[];
  logicToDeleteRelationNames: DataEnvDtoLogic[];
}

export interface DataEnv {
  matsToCreate: Materialization[];
  matsToReplace: Materialization[];
  matToDeleteRefs: MatToDeleteRef[];
  columnsToCreate: Column[];
  columnsToReplace: Column[];
  columnToDeleteRefs: ColToDeleteRef[];
  logicsToCreate: Logic[];
  logicsToReplace: Logic[];
  logicToDeleteRefs: LogicToDeleteRef[];
}

export interface DataEnvProps{
  dataEnv: DataEnv;
  catalog: ModelRepresentation[];
  dbCoveredNames: string[]
}
