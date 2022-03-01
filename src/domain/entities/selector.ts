export interface SelectorProperties {
  id: string;
  systemId: string;
  content: string;
  organizationId: string;
  modifiedOn?: number;
}

export class Selector {
  #id: string;

  #modifiedOn: number;

  #content: string;

  #organizationId: string;

  #systemId: string;

  public get id(): string {
    return this.#id;
  }

  public get content(): string {
    return this.#content;
  }

  public set content(content: string) {
    if (!content) throw new Error('Selector must have content');

    this.#content = content;
  }

  public get organizationId(): string {
    return this.#organizationId;
  }

  public set organizationId(organizationId: string) {
    if (!organizationId) throw new Error('Selector must have organizationId');

    this.#organizationId = organizationId;
  }

  public get systemId(): string {
    return this.#systemId;
  }

  public get modifiedOn(): number {
    return this.#modifiedOn;
  }

  public set modifiedOn(modifiedOn: number) {
    this.#modifiedOn = modifiedOn;
  }

  private constructor(properties: SelectorProperties) {
    this.#id = properties.id;
    this.#content = properties.content;
    this.#organizationId = properties.organizationId;
    this.#systemId = properties.systemId;
    this.#modifiedOn = properties.modifiedOn || Date.now();
  }

  public static create(properties: SelectorProperties): Selector {
    if (!properties.content) throw new Error('Selector must have content');
    if (!properties.organizationId)
      throw new Error('Selector must have organizationId');
    if (!properties.systemId) throw new Error('Selector must have system id');
    if (!properties.id) throw new Error('Selector must have id');

    const selector = new Selector(properties);
    return selector;
  }
}
