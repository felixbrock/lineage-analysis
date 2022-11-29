import { Column } from "../entities/column";
import { Logic} from "../entities/logic";
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

export interface DataEnv {
  matsToCreate?: Materialization[];
  matsToReplace?: Materialization[];
  matToDeleteRefs?: MatToDeleteRef[];
  columnsToCreate?: Column[];
  columnsToReplace?: Column[];
  columnToDeleteRefs?: ColToDeleteRef[];
  logicsToCreate?: Logic[];
  logicsToReplace?: Logic[];
  logicToDeleteRefs?: LogicToDeleteRef[];
}
