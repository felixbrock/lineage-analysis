export const externalResourceTypes = ['seed'] as const;
export type ExternalResourceType = typeof externalResourceTypes[number];

export const parseExternalResourceType = (
  typeToParse: string
): ExternalResourceType => {
  const identifiedElement = externalResourceTypes.find(
    (element) => element.toLowerCase() === typeToParse.toLowerCase()
  );
  if (identifiedElement) return identifiedElement;
  throw new Error('Provision of invalid type');
}; 

export interface ExternalResourceProperties {
  id: string;
  name: string;
  type: ExternalResourceType;
  lineageId: string;
  organizationId: string;
}

export class ExternalResource {
  #id: string;

  #name: string;

  #type: ExternalResourceType;

  #lineageId: string;

  #organizationId: string;

  get id(): string {
    return this.#id;
  }

  get name(): string {
    return this.#name;
  }

  get type(): ExternalResourceType {
    return this.#type;
  }

  get lineageId(): string {
    return this.#lineageId;
  }

  get organizationId(): string {
    return this.#organizationId;
  }

  private constructor(properties: ExternalResourceProperties) {
    this.#id = properties.id;
    this.#name = properties.name;
    this.#type = properties.type;
    this.#lineageId = properties.lineageId;
    this.#organizationId = properties.organizationId;
  }

  static create = (
    properties: ExternalResourceProperties
  ): ExternalResource => {
    if (!properties.id)
      throw new TypeError('ExternalResource object must have id');
    if (!properties.name)
      throw new TypeError('ExternalResource object must have name');
    if (!properties.type)
      throw new TypeError('ExternalResource object must have type');
    if (!properties.lineageId)
      throw new TypeError('ExternalResource object must have lineageId');
    if (!properties.organizationId)
      throw new TypeError('ExternalResource object must have oragnizationId');

    const externalresource = new ExternalResource(properties);

    return externalresource;
  };

  toDto = (): { [key: string]: any } => ({
    id: this.#id,
    name: this.#name,
    type: this.#type,
    lineageId: this.#lineageId,
    organizationId: this.#organizationId,
  });
}
