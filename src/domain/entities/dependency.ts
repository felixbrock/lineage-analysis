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

export interface DependencyProps {
  id: string;
  type: DependencyType;
  headId: string;
  tailId: string;
}

export interface DependencyPrototype {
  id: string;
  type: DependencyType;
  headId: string;
  tailId: string;
}

type DependencyDto = DependencyProps;

export class Dependency {
  #id: string;

  #type: DependencyType;

  #headId: string;

  #tailId: string;

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

  private constructor(properties: DependencyProps) {
    this.#id = properties.id;
    this.#type = properties.type;
    this.#headId = properties.headId;
    this.#tailId = properties.tailId;
  }

  static create = (prototype: DependencyPrototype): Dependency => {
    if (!prototype.id) throw new TypeError('Dependency object must have id');
    if (!prototype.type)
      throw new TypeError('Dependency object must have type');
    if (!prototype.headId)
      throw new TypeError('Dependency object must have headId');
    if (!prototype.tailId)
      throw new TypeError('Dependency object must have tailId');

    const dependency = new Dependency({
      ...prototype,
    });

    return dependency;
  };

  static build = (props: DependencyProps): Dependency => {
    if (!props.id) throw new TypeError('Dependency object must have id');
    if (!props.type) throw new TypeError('Dependency object must have type');
    if (!props.headId)
      throw new TypeError('Dependency object must have headId');
    if (!props.tailId)
      throw new TypeError('Dependency object must have tailId');

    return new Dependency(props);
  };

  toDto = (): DependencyDto => ({
    id: this.#id,
    type: this.#type,
    headId: this.#headId,
    tailId: this.#tailId,
  });
}
