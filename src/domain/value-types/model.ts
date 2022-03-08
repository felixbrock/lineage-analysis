export interface ModelProperties {
  sql: string;
  statementDependencies: [string, string][][];
}

export class Model {
  #sql: string;

  #statementDependencies: [string, string][][];

  get sql(): string {
    return this.#sql;
  }

  get statementDependencies(): [string, string][][] {
    return this.#statementDependencies;
  }

  private constructor(properties: ModelProperties) {
    this.#sql = properties.sql;
    this.#statementDependencies = properties.statementDependencies;
  }

  static create(properties: ModelProperties): Model {
    const model = new Model(properties);

    return model;
  }
}
