export enum DependencyType {
  DATA = 'DATA',
  QUERY = 'QUERY',
  DEFINITION = 'DEFINITION',
  EXTERNAL = 'EXTERNAL',
}

export interface DependencyProperties {
  id: string;
  type: DependencyType;
  headId: string;
  tailId: string;
  lineageIds: string[];
  organizationId: string;
}

type DependencyDto = DependencyProperties;

export class Dependency {
  #id: string;

  #type: DependencyType;

  #headId: string;

  #tailId: string;

  #lineageIds: string[];

  #organizationId: string;

  get id(): string {
    return this.#id;
  }

  get type(): DependencyType {
    return this.#type;
  }

  get headId(): string {
    return this.#headId;
  }

  get tailId(): string {
    return this.#tailId;
  }

  get lineageIds(): string[] {
    return this.#lineageIds;
  }

  get organizationId(): string {
    return this.#organizationId;
  }

  private constructor(properties: DependencyProperties) {
    this.#id = properties.id;
    this.#type = properties.type;
    this.#headId = properties.headId;
    this.#tailId = properties.tailId;
    this.#lineageIds = properties.lineageIds;
    this.#organizationId = properties.organizationId;
  }

  static create = (properties: DependencyProperties): Dependency => {
    if (!properties.id) throw new TypeError('Dependency object must have id');
    if (!properties.type)
      throw new TypeError('Dependency object must have type');
    if (!properties.headId)
      throw new TypeError('Dependency object must have headId');
    if (!properties.tailId)
      throw new TypeError('Dependency object must have tailId');
    if (!properties.lineageIds.length)
      throw new TypeError('Dependency object must have lineageId');
    if (!properties.organizationId)
      throw new TypeError('Dependency object must have oragnizationId');

    const dependency = new Dependency(properties);

    return dependency;
  };

  toDto = (): DependencyDto => ({
    id: this.#id,
    type: this.#type,
    headId: this.#headId,
    tailId: this.#tailId,
    lineageIds: this.#lineageIds,
    organizationId: this.#organizationId,
  });
}
