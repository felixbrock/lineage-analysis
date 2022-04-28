export enum MaterializationType {
  TABLE = 'BASE TABLE',
  VIEW = 'VIEW',
}

export interface MaterializationProperties {
  id: string;
  dbtModelId: string;
  name: string;
  schemaName: string;
  databaseName: string;
  materializationType: MaterializationType;
  logicId: string;
  lineageId: string;
}

export class Materialization {
  #id: string;

  #dbtModelId: string;

  #name: string;

  #schemaName: string;

  #databaseName: string;

  #materializationType: MaterializationType;

  #logicId: string;

  #lineageId: string;

  get id(): string {
    return this.#id;
  }

  get dbtModelId(): string {
    return this.#dbtModelId;
  }

  get name(): string {
    return this.#name;
  }

  get schemaName(): string {
    return this.#schemaName;
  }

  get databaseName(): string {
    return this.#databaseName;
  }

  get materializationType(): MaterializationType {
    return this.#materializationType;
  }

  get logicId(): string {
    return this.#logicId;
  }

  get lineageId(): string {
    return this.#lineageId;
  }

  private constructor(properties: MaterializationProperties) {
    this.#id = properties.id;
    this.#dbtModelId = properties.dbtModelId;
    this.#name = properties.name;
    this.#schemaName = properties.schemaName;
    this.#databaseName = properties.databaseName;
    this.#materializationType = properties.materializationType;
    this.#logicId = properties.logicId;
    this.#lineageId = properties.lineageId;
  }

  static create = (properties: MaterializationProperties): Materialization => {
    if (!properties.id) throw new TypeError('Materialization must have id');
    if (!properties.dbtModelId)
      throw new TypeError('Materialization must have dbtModelId');
    if (!properties.name) throw new TypeError('Materialization must have name');
    if (!properties.schemaName)
      throw new TypeError('Materialization must have schema name');
    if (!properties.databaseName)
      throw new TypeError('Materialization must have database name');
    if (!properties.materializationType)
      throw new TypeError('Materialization must have materialization type');
    if (!properties.logicId) throw new TypeError('Materialization must have logicId');
    if (!properties.lineageId) throw new TypeError('Materialization must have lineageId');

    const materialization = new Materialization(properties);

    return materialization;
  };
}
