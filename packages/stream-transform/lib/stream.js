/*
Stream Transform - Web Streams API

Please look at the project documentation for
additional information.
*/

const transform = function (handler, options = {}) {
  // Normalize the options, following the Node.js implementation
  options = { ...options };
  if (options.parallel === undefined || options.parallel === null) {
    options.parallel = 100;
  }
  if (options.params === undefined || options.params === null) {
    options.params = null;
  }
  // The handler signature defines its mode, synchronous handlers and
  // handlers returning a promise declare one argument, asynchronous
  // handlers declare an additional callback argument.
  let l = handler.length;
  if (options.params !== null) {
    l--;
  }
  if (l !== 1 && l !== 2) {
    throw Error("Invalid handler arguments");
  }
  // Information
  const state = {
    running: 0,
    started: 0,
    finished: 0,
  };
  // The first error reported by a handler, mirrors the `destroy` call of
  // the Node.js implementation
  let error;
  let controller;
  // Waiters are released every time a handler terminates, there is at most
  // one waiter at a time, registered by `transform` to apply backpressure
  // or by `flush` to wait for the handlers still running.
  const waiters = [];
  const done = (err, chunks) => {
    state.running--;
    if (err) {
      if (error === undefined) {
        error = err;
        controller.error(err);
      }
    } else {
      state.finished++;
      for (const chunk of chunks) {
        // Skip null and undefined records, like the Node.js implementation
        if (chunk !== undefined && chunk !== null) {
          try {
            controller.enqueue(chunk);
          } catch {
            // The stream was cancelled or errored while the handler was
            // running, the chunk is discarded.
          }
        }
      }
    }
    while (waiters.length !== 0) {
      waiters.shift()();
    }
  };
  const execute = (chunk) => {
    state.started++;
    state.running++;
    try {
      if (l === 1) {
        // Synchronous handler, the result may be a promise
        const result = handler(chunk, options.params);
        if (result && typeof result.then === "function") {
          result.then(
            (result) => done(null, [result]),
            (err) => done(err),
          );
        } else {
          done(null, [result]);
        }
      } else {
        // Asynchronous handler with callback
        handler(chunk, (err, ...chunks) => done(err, chunks), options.params);
      }
    } catch (err) {
      done(err);
    }
  };
  return new TransformStream(
    {
      start(ctr) {
        controller = ctr;
      },
      transform(chunk) {
        if (error !== undefined) {
          throw error;
        }
        execute(chunk);
        // Return immediately while there is room for more handlers to run
        // in parallel, otherwise apply backpressure until one terminates.
        if (state.running < options.parallel) {
          return;
        }
        return new Promise((resolve, reject) => {
          waiters.push(() => {
            if (error !== undefined) {
              reject(error);
            } else {
              resolve();
            }
          });
        });
      },
      async flush() {
        // Wait for the handlers still running
        while (state.running !== 0) {
          await new Promise((resolve, reject) => {
            waiters.push(() => {
              if (error !== undefined) {
                reject(error);
              } else {
                resolve();
              }
            });
          });
        }
        if (error !== undefined) {
          throw error;
        }
      },
    },
    new CountQueuingStrategy({ highWaterMark: 1024 }),
    new CountQueuingStrategy({ highWaterMark: 1024 }),
  );
};

export { transform };
