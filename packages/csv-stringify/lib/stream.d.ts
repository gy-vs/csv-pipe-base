import { Options } from "./index.js";

declare function stringify<T = unknown>(
  options?: Options,
): TransformStream<T, string>;

export { stringify };

export { CsvError, CsvErrorCode } from "./api/CsvError.js";

export {
  Cast,
  CastingContext,
  CastReturnObject,
  ColumnOption,
  Input,
  Options,
  OptionsNormalized,
  RecordDelimiter,
} from "./index.js";
