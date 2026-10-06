/*
CSV Stringify - Web Streams API

Please look at the project documentation for
additional information.
*/

import { CsvError } from "./api/CsvError.js";
import { stringifier } from "./api/index.js";
import { normalize_options } from "./api/normalize_options.js";

// The API emits the BOM as a Buffer in Node.js, every other chunk is a
// string; decode it so that the stream only enqueues strings of CSV text.
// Note, `ignoreBOM` preserves the BOM, which the decoder would otherwise
// strip from the decoded text.
const decoder = new TextDecoder("utf-8", { ignoreBOM: true });

const stringify = function (opts = {}) {
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
  const push = (chunk) => {
    controller.enqueue(
      typeof chunk === "string" ? chunk : decoder.decode(chunk),
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
          // Note, the stream might have been errored inside `transform`,
          // we must prevent any chunk from being pushed after an error.
          return;
        }
        if (info.records === 0) {
          api.bom(push);
          const err = api.headers(push);
          if (err) controller.error(err);
        }
      },
    },
    new CountQueuingStrategy({ highWaterMark: 1024 }),
    new CountQueuingStrategy({ highWaterMark: 1024 }),
  );
};

export { stringify, CsvError };
