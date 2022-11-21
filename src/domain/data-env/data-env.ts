import { Column } from "../entities/column";
import { Logic} from "../entities/logic";
import { Materialization } from "../entities/materialization";

export interface DataEnv {
  matsToCreate?: Materialization[];
  matsToReplace?: Materialization[];
  matsToRemove?: Materialization[];
  columnsToCreate?: Column[];
  columnsToReplace?: Column[];
  columnsToRemove?: Column[];
  logicsToCreate?: Logic[];
  logicsToReplace?: Logic[];
  logicsToRemove?: Logic[];
}
