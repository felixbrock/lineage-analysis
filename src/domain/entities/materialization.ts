export const materializationTypes = ['base table', 'view'] as const;
export type MaterializationType = typeof materializationTypes[number];

export const parseMaterializationType = (
  materializationType: string
): MaterializationType => {
  const identifiedElement = materializationTypes.find(
    (element) => element.toLowerCase() === materializationType.toLowerCase()
  );
  if (identifiedElement) return identifiedElement;
  throw new Error('Provision of invalid type');
};

export interface MaterializationProperties {
  id: string;
  relationName: string;
  name: string;
  schemaName: string;
  databaseName: string;
  materializationType: MaterializationType;
  logicId?: string;
  lineageIds: string[];
  organizationId: string;
}

type MaterializationDto = MaterializationProperties;

export class Materialization {
  #id: string;

  #relationName: string;

  #name: string;

  #schemaName: string;

  #databaseName: string;

  #materializationType: MaterializationType;

  #logicId?: string;

  #lineageIds: string[];

  #organizationId: string;

  get id(): string {
    return this.#id;
  }

  get relationName(): string {
    return this.#relationName;
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

  get logicId(): string | undefined {
    return this.#logicId;
  }

  get lineageIds(): string[] {
    return this.#lineageIds;
  }

  get organizationId(): string {
    return this.#organizationId;
  }

  private constructor(properties: MaterializationProperties) {
    this.#id = properties.id;
    this.#relationName = properties.relationName;
    this.#name = properties.name;
    this.#schemaName = properties.schemaName;
    this.#databaseName = properties.databaseName;
    this.#materializationType = properties.materializationType;
    this.#logicId = properties.logicId;
    this.#lineageIds = properties.lineageIds;
    this.#organizationId = properties.organizationId;
  }

  static create = (properties: MaterializationProperties): Materialization => {
    if (!properties.id) throw new TypeError('Materialization must have id');
    if (!properties.relationName)
      throw new TypeError('Materialization must have relationName');
    if (!properties.name) throw new TypeError('Materialization must have name');
    if (!properties.schemaName)
      throw new TypeError('Materialization must have schema name');
    if (!properties.databaseName)
      throw new TypeError('Materialization must have database name');
    if (!properties.materializationType)
      throw new TypeError('Materialization must have materialization type');
    if (!properties.lineageIds.length)
      throw new TypeError('Materialization must have lineageId');
    if (!properties.organizationId)
      throw new TypeError('Materialization must have organization id');

    const materialization = new Materialization(properties);

    return materialization;
  };

  toDto = (): MaterializationDto => ({
    id: this.#id,
    relationName: this.#relationName,
    materializationType: this.#materializationType,
    name: this.#name,
    schemaName: this.#schemaName,
    databaseName: this.#databaseName,
    logicId: this.#logicId,
    lineageIds: this.#lineageIds,
    organizationId: this.#organizationId,
  });
}
