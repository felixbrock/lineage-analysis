export type StatementReference = [string,string];

export interface ModelProperties {
  id: string
  sql: string;
  statementReferences: StatementReference[][];
}

export class Model {
  #id: string;

  #sql: string;

  #statementReferences: StatementReference[][];

  get id(): string {
    return this.#id
  }

  get sql(): string {
    return this.#sql;
  }

  get statementReferences(): StatementReference[][] {
    return this.#statementReferences;
  }

  private constructor(properties: ModelProperties) {
    this.#id = properties.id;
    this.#sql = properties.sql;
    this.#statementReferences = properties.statementReferences;
  }

  static create(properties: ModelProperties): Model {
    const model = new Model(properties);

    return model;
  }
}
