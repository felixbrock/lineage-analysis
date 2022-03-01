import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { SelectorDto } from './selector-dto';

export interface ReadSelectorRequestDto {
  id: string;
}

export interface ReadSelectorAuthDto {
  organizationId: string;
}

export type ReadSelectorResponseDto = Result<SelectorDto>;

export class ReadSelector
  implements
    IUseCase<
      ReadSelectorRequestDto,
      ReadSelectorResponseDto,
      ReadSelectorAuthDto
    >
{

  public constructor() {
    console.log('read selector constructor');
  }

  // eslint-disable-next-line class-methods-use-this
  public async execute(
    request: ReadSelectorRequestDto,
    auth: ReadSelectorAuthDto
  ): Promise<ReadSelectorResponseDto> {
    try {
      console.log('find selector');
      if (!'TODO')
        throw new Error(`Selector with id ${request.id} does not exist`);

      if (auth.organizationId !== 'TODO')
        throw new Error('Not authorized to perform action');

      return Result.ok({content:'', id:'', modifiedOn:0, organizationId:'', systemId:''});
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
