export type DeletionMode = 'soft';

export interface IObservabilityApiRepo {
  deleteQuantTestSuites(
    jwt: string,
    targetResourceIds: string[],
    mode: DeletionMode
  ): Promise<void>;
  deleteQualTestSuites(
    jwt: string,
    targetResourceIds: string[],
    mode: DeletionMode
  ): Promise<void>;
}
