import { TransformStream, CountQueuingStrategy } from "node:stream/web";
import { CsvError } from "./api/CsvError.js";
import { stringifier } from "./api/index.js";
import { normalize_options } from "./api/normalize_options.js";

const stringify = (opts = {}) => {
  const [err, options] = normalize_options(opts);
  if (err !== undefined) throw err;
  // Internal state
  const state = {
    stop: false,
  };
  // Information
  const info = {
    records: 0,
  };
  const api = stringifier(options, state, info);
  let controller;
  const encoder = new TextEncoder();
  const push = (chunk) => {
    // Always emit binary chunks: the BOM is a `Uint8Array` and text chunks are
    // encoded as UTF-8. This keeps the output byte identical to the Node.js
    // implementation while being directly consumable by a `Response` or any
    // byte oriented `WritableStream`.
    controller.enqueue(
      typeof chunk === "string" ? encoder.encode(chunk) : chunk,
    );
  };
  return new TransformStream(
    {
      start(ctr) {
        controller = ctr;
      },
      transform(chunk) {
        if (state.stop === true) {
          return;
        }
        const err = api.__transform(chunk, push);
        if (err !== undefined) {
          state.stop = true;
          controller.error(err);
        }
      },
      flush() {
        if (state.stop === true) {
          // Mirrors the Node.js implementation which skips flush after an
          // error, otherwise the stream would be closed after being errored.
          return;
        }
        if (info.records === 0) {
          api.bom(push);
          const err = api.headers(push);
          if (err) {
            state.stop = true;
            controller.error(err);
          }
        }
      },
    },
    new CountQueuingStrategy({ highWaterMark: 1024 }),
    new CountQueuingStrategy({ highWaterMark: 1024 }),
  );
};

export { stringify, CsvError, normalize_options };
