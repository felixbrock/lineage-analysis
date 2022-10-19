export const dependencyTypes = [
  'data',
  'query',
  'definition',
  'external',
] as const;
export type DependencyType = typeof dependencyTypes[number];

export const parseDependencyType = (dependencyType: string): DependencyType => {
  const identifiedElement = dependencyTypes.find(
    (element) => element.toLowerCase() === dependencyType.toLowerCase()
  );
  if (identifiedElement) return identifiedElement;
  throw new Error('Provision of invalid type');
};

export interface DependencyProperties {
  id: string;
  type: DependencyType;
  headId: string;
  tailId: string;
  lineageIds: string[];
  organizationId: string;
}

export interface DependencyPrototype {
  id: string;
  type: DependencyType;
  headId: string;
  tailId: string;
  lineageId: string;
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

  static create = (prototype: DependencyPrototype): Dependency => {
    if (!prototype.id) throw new TypeError('Dependency object must have id');
    if (!prototype.type)
      throw new TypeError('Dependency object must have type');
    if (!prototype.headId)
      throw new TypeError('Dependency object must have headId');
    if (!prototype.tailId)
      throw new TypeError('Dependency object must have tailId');
    if (!prototype.lineageId)
      throw new TypeError('Dependency object must have lineageId');
    if (!prototype.organizationId)
      throw new TypeError('Dependency object must have oragnizationId');

    const dependency = new Dependency({
      ...prototype,
      lineageIds: [prototype.lineageId],
    });

    return dependency;
  };

  static build = (props: DependencyProperties): Dependency => {
    if (!props.id) throw new TypeError('Dependency object must have id');
    if (!props.type) throw new TypeError('Dependency object must have type');
    if (!props.headId)
      throw new TypeError('Dependency object must have headId');
    if (!props.tailId)
      throw new TypeError('Dependency object must have tailId');
    if (!props.lineageIds.length)
      throw new TypeError('Dependency object must have lineageIds');
    if (!props.organizationId)
      throw new TypeError('Dependency object must have oragnizationId');

    return new Dependency(props);
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
