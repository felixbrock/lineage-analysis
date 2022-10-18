export interface LineagePrototype {
  id: string;
  createdAt?: string;
  organizationId: string;
  completed?: boolean;
}

export interface LineageProperties
  extends Omit<LineagePrototype, 'createdAt' | 'completed'> {
  createdAt: string;
  completed: boolean;
}

type LineageDto = LineageProperties;

export class Lineage {
  #id: string;

  #createdAt: string;

  #organizationId: string;

  #completed: boolean;

  get id(): string {
    return this.#id;
  }

  get createdAt(): string {
    return this.#createdAt;
  }

  get organizationId(): string {
    return this.#organizationId;
  }

  get completed(): boolean {
    return this.#completed;
  }

  private constructor(props: LineageProperties) {
    this.#id = props.id;
    this.#createdAt = props.createdAt;
    this.#organizationId = props.organizationId;
    this.#completed = props.completed;
  }

  static create = (prototype: LineagePrototype): Lineage => {
    if (!prototype.id) throw new TypeError('Lineage must have id');
    if (!prototype.organizationId)
      throw new TypeError('Lineage must have organization id');

    const lineage = Lineage.build({
      ...prototype,
      createdAt: prototype.createdAt || new Date().toISOString(),
      completed: prototype.completed || false,
    });

    return lineage;
  };

  static build = (props: LineageProperties): Lineage =>
    new Lineage({
      id: props.id,
      organizationId: props.organizationId,
      createdAt: props.createdAt,
      completed: props.completed,
    });

  toDto = (): LineageDto => ({
    id: this.#id,
    createdAt: this.#createdAt,
    organizationId: this.#organizationId,
    completed: this.#completed,
  });
}
