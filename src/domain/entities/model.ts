export type StatementReference = [string, string];

export interface ModelProperties {
  id: string;
  location: string;
  sql: string;
  statementReferences: StatementReference[][];
}

export class Model {
  #id: string;

  #location: string;

  #sql: string;

  #statementReferences: StatementReference[][];

  get id(): string {
    return this.#id;
  }

  get location(): string {
    return this.#location;
  }
  get sql(): string {
    return this.#sql;
  }

  get statementReferences(): StatementReference[][] {
    return this.#statementReferences;
  }

  private constructor(properties: ModelProperties) {
    this.#id = properties.id;
    this.#location = properties.location;
    this.#sql = properties.sql;
    this.#statementReferences = properties.statementReferences;
  }

  static create(properties: ModelProperties): Model {
    const model = new Model(properties);

    return model;
  }
}
