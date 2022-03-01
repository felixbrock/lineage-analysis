import { Selector } from '../entities/selector';

export interface SelectorDto {
  id: string;
  content: string;
  organizationId: string;
  systemId: string;
  modifiedOn: number;
}

export const buildSelectorDto = (selector: Selector): SelectorDto => ({
  id: selector.id,
  modifiedOn: selector.modifiedOn,
  content: selector.content,
  organizationId: selector.organizationId,
  systemId: selector.systemId,
});
