export interface LineagePrototype {
  id: string;
  createdAt?: string;
  organizationId: string;
  finished?: boolean;
}

interface LineageProperties
  extends Omit<LineagePrototype, 'createdAt' | 'finished'> {
  createdAt: string;
  finished: boolean;
}

type LineageDto = LineageProperties;

export class Lineage {
  #id: string;

  #createdAt: string;

  #organizationId: string;

  #finished: boolean;

  get id(): string {
    return this.#id;
  }

  get createdAt(): string {
    return this.#createdAt;
  }

  get organizationId(): string {
    return this.#organizationId;
  }

  get finished(): boolean {
    return this.#finished;
  }

  private constructor(props: LineageProperties) {
    this.#id = props.id;
    this.#createdAt = props.createdAt;
    this.#organizationId = props.organizationId;
    this.#finished = props.finished;
  }

  static create = (prototype: LineagePrototype): Lineage => {
    if (!prototype.id) throw new TypeError('Lineage must have id');
    if (!prototype.organizationId)
      throw new TypeError('Lineage must have organization id');

    const lineage = this.#build(prototype);

    return lineage;
  };

  static #build = (prototype: LineagePrototype): Lineage =>
    new Lineage({
      id: prototype.id,
      organizationId: prototype.organizationId,
      createdAt: prototype.createdAt || new Date().toISOString(),
      finished: prototype.finished || false,
    });

  toDto = (): LineageDto => ({
    id: this.#id,
    createdAt: this.#createdAt,
    organizationId: this.#organizationId,
    finished: this.#finished,
  });
}
