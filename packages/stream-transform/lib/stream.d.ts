import { HandlerCallback, Options } from "./index.js";

/**
 * Handler for synchronous and promise based transformations, the `params`
 * argument is only present when the `params` option is defined.
 */
export type HandlerSync<T = unknown, U = unknown, P = unknown> = (
  record: T,
  params?: P,
) => U | Promise<U>;

/**
 * Handler for callback based transformations, the `params` argument is
 * only present when the `params` option is defined.
 */
export type HandlerAsync<T = unknown, U = unknown, P = unknown> = (
  record: T,
  callback: HandlerCallback<U>,
  params?: P,
) => void;

declare function transform<T = unknown, U = unknown, P = unknown>(
  handler: HandlerSync<T, U, P>,
  options?: Options & { params?: P },
): TransformStream<T, U>;
declare function transform<T = unknown, U = unknown, P = unknown>(
  handler: HandlerAsync<T, U, P>,
  options?: Options & { params?: P },
): TransformStream<T, U>;

export { transform };

export { HandlerCallback, Options } from "./index.js";
