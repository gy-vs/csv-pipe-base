import { Options } from "./index.js";

// A record is either an array or a plain object, mirrors the runtime
// validation performed by the stringifier.
export type InputRecord = unknown[] | { [key: string]: unknown };

declare function stringify<T extends InputRecord = unknown[]>(
  options?: Options,
): TransformStream<T, Uint8Array>;

export { stringify };

export {
  Callback,
  RecordDelimiter,
  CastReturnObject,
  Cast,
  PlainObject,
  Input,
  ColumnOption,
  CastingContext,
  OptionsNormalized,
  Options,
  Stringifier,
} from "./index.js";
