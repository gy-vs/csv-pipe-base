import { TransformStream, CountQueuingStrategy } from "node:stream/web";

// Accept `transform(handler, options)` and `transform(options, handler)`,
// mirrors the argument handling of the Node.js implementation.
const get_arguments = function (a, b) {
  if (typeof a === "function") {
    return [a, b];
  } else if (typeof b === "function") {
    return [b, a];
  } else {
    throw Error("Invalid handler arguments");
  }
};

const transform = function (a, b) {
  const [handler, opts] = get_arguments(a, b);
  let options = {};
  if (opts !== undefined && opts !== null) {
    options = { ...opts };
  }
  if (options.parallel === undefined || options.parallel === null) {
    options.parallel = 100;
  }
  if (options.params === undefined || options.params === null) {
    options.params = null;
  }
  const state = {
    running: 0,
    started: 0,
    finished: 0,
  };
  // Context exposed as `this` inside the handler, mirrors the Node.js
  // implementation which binds the handler to the Transformer instance.
  const context = {
    options,
    state,
  };
  const parallel = options.parallel;
  const params = options.params;
  // Chunks admitted from the writable side but waiting for a free execution
  // slot. A write resolves as soon as its chunk enters a slot (the handler is
  // called), exactly like the Node.js implementation invokes the transform
  // callback; the chunk may then complete asynchronously and out of order.
  const queue = [];
  let closing = false;
  let failed = false;
  let finish_resolve, finish_reject;
  let controller;

  const finish = function () {
    if (finish_resolve !== undefined) {
      finish_resolve();
      finish_resolve = undefined;
      finish_reject = undefined;
    }
  };

  const fail = function (err) {
    if (failed) {
      return;
    }
    failed = true;
    // Reject the chunks still waiting for a slot together with the writes
    // behind them; chunks already admitted completed or error the stream.
    for (const item of queue) {
      item.reject(err);
    }
    queue.length = 0;
    controller.error(err);
    if (finish_reject !== undefined) {
      finish_reject(err);
      finish_resolve = undefined;
      finish_reject = undefined;
    }
  };

  // Fill free execution slots with queued chunks. Runs synchronously as much
  // as possible, so synchronous handlers never recurse.
  const pump = function () {
    while (!failed && state.running < parallel && queue.length > 0) {
      const item = queue.shift();
      state.started++;
      state.running++;
      const done = function (err, chunks, coerce) {
        state.running--;
        if (failed) {
          return;
        }
        if (err) {
          return fail(err);
        }
        state.finished++;
        for (let chunk of chunks) {
          // Only chunks emitted through the callback API are coerced, mirrors
          // the Node.js implementation
          if (coerce && typeof chunk === "number") {
            chunk = `${chunk}`;
          }
          // Empty values are skipped, mirrors the Node.js implementation
          if (chunk !== undefined && chunk !== null && chunk !== "") {
            controller.enqueue(chunk);
          }
        }
        if (closing && state.running === 0 && queue.length === 0) {
          finish();
        }
        // A slot just freed, admit another chunk if any is queued
        if (queue.length > 0) {
          pump();
        }
      };
      try {
        let l = handler.length;
        if (params !== null) {
          l--;
        }
        if (l === 1) {
          // sync or promise handler
          const result = handler.call(context, item.chunk, params);
          // The chunk is now admitted, the write may resolve
          item.resolve();
          if (result !== null && typeof result.then === "function") {
            Promise.resolve(result).then(
              (result) => {
                done(null, [result], false);
              },
              (err) => {
                done(err);
              },
            );
          } else {
            done(null, [result], false);
          }
        } else if (l === 2) {
          // callback handler
          const callback = (err, ...chunks) => done(err, chunks, true);
          handler.call(context, item.chunk, callback, params);
          item.resolve();
        } else {
          throw Error("Invalid handler arguments");
        }
      } catch (err) {
        item.reject(err);
        fail(err);
      }
    }
  };

  return new TransformStream(
    {
      start(ctr) {
        controller = ctr;
      },
      transform(chunk) {
        if (failed) {
          return Promise.reject(new Error("Stream is errored"));
        }
        return new Promise((resolve, reject) => {
          queue.push({ chunk, resolve, reject });
          pump();
        });
      },
      flush() {
        closing = true;
        if (state.running === 0 && queue.length === 0) {
          return;
        }
        return new Promise((resolve, reject) => {
          finish_resolve = resolve;
          finish_reject = reject;
        });
      },
    },
    new CountQueuingStrategy({ highWaterMark: 1024 }),
    new CountQueuingStrategy({ highWaterMark: 1024 }),
  );
};

export { transform };
