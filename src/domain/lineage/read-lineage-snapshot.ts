import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { ILineageRepo } from './i-lineage-repo';
import { IMaterializationRepo } from '../materialization/i-materialization-repo';
import { IColumnRepo } from '../column/i-column-repo';
import { IDependencyRepo } from '../dependency/i-dependency-repo';
import { LineageDto, buildLineageDto } from './lineage-dto';
import {
  buildMaterializationDto,
  MaterializationDto,
} from '../materialization/materialization-dto';
import { buildColumnDto, ColumnDto } from '../column/column-dto';
import {
  buildDependencyDto,
  DependencyDto,
} from '../dependency/dependency-dto';

export interface ReadLineageSnapshotRequestDto {
  lineageId: string;
}

export interface ReadLineageSnapshotAuthDto {
  organizationId: string;
}

interface LineageSnapshot {
  lineage: LineageDto;
  materializations: MaterializationDto[];
  columns: ColumnDto[];
  dependencies: DependencyDto[];
}

export type ReadLineageSnapshotResponseDto = Result<LineageSnapshot>;

export class ReadLineageSnapshot
  implements
    IUseCase<ReadLineageSnapshotRequestDto, ReadLineageSnapshotResponseDto, ReadLineageSnapshotAuthDto>
{
  readonly #lineageRepo: ILineageRepo;

  readonly #materializationRepo: IMaterializationRepo;

  readonly #columnRepo: IColumnRepo;

  readonly #dependencyRepo: IDependencyRepo;

  constructor(
    lineageRepo: ILineageRepo,
    materializationRepo: IMaterializationRepo,
    columnRepo: IColumnRepo,
    dependencyRepo: IDependencyRepo
  ) {
    this.#lineageRepo = lineageRepo;
    this.#materializationRepo = materializationRepo;
    this.#columnRepo = columnRepo;
    this.#dependencyRepo = dependencyRepo;
  }

  async execute(
    request: ReadLineageSnapshotRequestDto,
    auth: ReadLineageSnapshotAuthDto
  ): Promise<ReadLineageSnapshotResponseDto> {
    try {
      // todo -replace
      console.log(auth);

      const lineage = await this.#lineageRepo.findOne(request.lineageId);
      if (!lineage)
        throw new Error(`Lineage with id ${request.lineageId} does not exist`);

      const materializations = await this.#materializationRepo.findBy({
        lineageId: request.lineageId,
      });
      const columns = await this.#columnRepo.findBy({ lineageId: request.lineageId });
      const dependencies = await this.#dependencyRepo.findBy({
        lineageId: request.lineageId,
      });

      // if (lineage.organizationId !== auth.organizationId)
      //   throw new Error('Not authorized to perform action');

      return Result.ok({
        lineage: buildLineageDto(lineage),
        materializations: materializations.map((materialization) =>
          buildMaterializationDto(materialization)
        ),
        columns: columns.map((column) => buildColumnDto(column)),
        dependencies: dependencies.map((dependency) =>
          buildDependencyDto(dependency)
        ),
      });
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
