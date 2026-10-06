import { Options, State } from "./index.js";

export interface HandlerContext {
  readonly options: Options;
  readonly state: State;
}

export type SyncHandler<T, U> = (
  this: HandlerContext,
  record: T,
  params?: unknown,
) => U | PromiseLike<U>;

export type CallbackHandler<T, U> = (
  this: HandlerContext,
  record: T,
  callback: (err?: null | Error, record?: U) => void,
  params?: unknown,
) => void;

declare function transform<T, U>(
  handler: SyncHandler<T, U>,
  options?: Options,
): TransformStream<T, Exclude<Awaited<U>, null | undefined>>;
declare function transform<T, U>(
  options: Options,
  handler: SyncHandler<T, U>,
): TransformStream<T, Exclude<Awaited<U>, null | undefined>>;
declare function transform<T, U>(
  handler: CallbackHandler<T, U>,
  options?: Options,
): TransformStream<T, U>;
declare function transform<T, U>(
  options: Options,
  handler: CallbackHandler<T, U>,
): TransformStream<T, U>;

export { transform };

export {
  Handler,
  HandlerCallback,
  Callback,
  Options,
  State,
  Transformer,
} from "./index.js";
