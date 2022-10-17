interface QueryHistoryEntryDto {
  QUERY_TEXT: string;
  [key: string]: unknown;
}

export interface QueryHistoryDto {
    [key: string]: QueryHistoryEntryDto[];
}
