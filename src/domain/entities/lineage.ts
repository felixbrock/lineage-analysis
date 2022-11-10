export interface LineagePrototype {
  id: string;
  createdAt?: string;
}

export interface LineageProperties
  extends Omit<LineagePrototype, 'createdAt'> {
  createdAt: string;
  completed: boolean;
}

type LineageDto = LineageProperties;

export class Lineage {
  #id: string;

  #createdAt: string;


  #completed: boolean;

  get id(): string {
    return this.#id;
  }

  get createdAt(): string {
    return this.#createdAt;
  }

  get completed(): boolean {
    return this.#completed;
  }

  private constructor(props: LineageProperties) {
    this.#id = props.id;
    this.#createdAt = props.createdAt;
    this.#completed = props.completed;
  }

  static create = (prototype: LineagePrototype): Lineage => {
    if (!prototype.id) throw new TypeError('Lineage must have id');

    const lineage = Lineage.build({
      ...prototype,
      createdAt: prototype.createdAt || new Date().toISOString(),
      completed: false,
    });

    return lineage;
  };

  static build = (props: LineageProperties): Lineage =>
    new Lineage({
      id: props.id,
      createdAt: props.createdAt,
      completed: props.completed,
    });

  toDto = (): LineageDto => ({
    id: this.#id,
    createdAt: this.#createdAt,
    completed: this.#completed,
  });
}
