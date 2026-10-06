export type CsvErrorCode = string;

export class CsvError extends Error {
  readonly code: CsvErrorCode;
  [key: string]: unknown;

  constructor(
    code: CsvErrorCode,
    message: string | string[],
    ...contexts: unknown[]
  );
}
