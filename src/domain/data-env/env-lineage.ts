import { Dependency } from '../entities/dependency';

export interface EnvLineage {
  dependenciesToCreate: Dependency[];
}
