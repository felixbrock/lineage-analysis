export interface ModelProperties {
  sql: string;
  statementReferences: [string, string][][];
}

export class Model {
  #sql: string;

  #statementReferences: [string, string][][];

  get sql(): string {
    return this.#sql;
  }

  get statementReferences(): [string, string][][] {
    return this.#statementReferences;
  }

  private constructor(properties: ModelProperties) {
    this.#sql = properties.sql;
    this.#statementReferences = properties.statementReferences;
  }

  static create(properties: ModelProperties): Model {
    const model = new Model(properties);

    return model;
  }
}
