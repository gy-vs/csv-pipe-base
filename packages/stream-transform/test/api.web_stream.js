import "should";
import { transform } from "../lib/stream.js";
import { transform as transformNode } from "../lib/index.js";

// Run records through a Web TransformStream and return the emitted chunks
const pipe = async (records, stream) => {
  const writer = stream.writable.getWriter();
  const chunks = [];
  const reading = (async () => {
    const reader = stream.readable.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }
  })();
  const writing = (async () => {
    for (const record of records) {
      await writer.write(record);
    }
    await writer.close();
  })();
  await Promise.all([reading, writing]);
  return chunks;
};

// Like `pipe` but resolves with the error instead of rejecting
const pipeError = async (records, stream) => {
  try {
    await pipe(records, stream);
    return Error("Shall not be called");
  } catch (err) {
    return err;
  }
};

describe("API Web Stream", function () {
  describe("handlers", function () {
    it("handle synchronous handlers", async function () {
      const chunks = await pipe(
        ["a", "b", "c"],
        transform((record) => record.toUpperCase()),
      );
      chunks.should.eql(["A", "B", "C"]);
    });

    it("handle handlers returning a promise", async function () {
      const chunks = await pipe(
        ["a", "b", "c"],
        transform(async (record) => {
          await new Promise((resolve) => setTimeout(resolve, 1));
          return record.toUpperCase();
        }),
      );
      chunks.should.eql(["A", "B", "C"]);
    });

    it("handle handlers with a callback", async function () {
      const chunks = await pipe(
        ["a", "b", "c"],
        transform((record, callback) => {
          setTimeout(() => callback(null, record.toUpperCase()), 1);
        }),
      );
      chunks.should.eql(["A", "B", "C"]);
    });

    it("throw on invalid handler arguments", function () {
      (() => {
        transform(() => {});
      }).should.throw("Invalid handler arguments");
    });
  });

  describe("records", function () {
    it("filter null and undefined records, like the Node.js implementation", async function () {
      const chunks = await pipe(
        [1, 2, 3, 4, 5],
        transform((record) => {
          if (record === 2) return null;
          if (record === 4) return undefined;
          return record;
        }),
      );
      chunks.should.eql([1, 3, 5]);
    });

    it("emit the records returned by the handler untouched", async function () {
      const chunks = await pipe(
        [1, 2, 3],
        transform((record) => [record, `${record}`]),
      );
      chunks.should.eql([
        [1, "1"],
        [2, "2"],
        [3, "3"],
      ]);
    });

    it("wait for running handlers before closing", async function () {
      const chunks = await pipe(
        [1, 2, 3],
        transform(async (record) => {
          await new Promise((resolve) => setTimeout(resolve, 10));
          return record;
        }),
      );
      chunks.sort().should.eql([1, 2, 3]);
    });
  });

  describe("options", function () {
    it("params with synchronous handlers", async function () {
      const chunks = await pipe(
        ["a", "b"],
        transform((record, params) => `${record}:${params.suffix}`, {
          params: { suffix: "!" },
        }),
      );
      chunks.should.eql(["a:!", "b:!"]);
    });

    it("params with callback handlers", async function () {
      const chunks = await pipe(
        ["a", "b"],
        transform(
          (record, callback, params) => {
            setImmediate(() => callback(null, `${record}:${params.suffix}`));
          },
          { params: { suffix: "!" } },
        ),
      );
      chunks.should.eql(["a:!", "b:!"]);
    });

    it("parallel runs handlers concurrently", async function () {
      let running = 0;
      let running_max = 0;
      const chunks = await pipe(
        [1, 2, 3, 4, 5, 6, 7, 8],
        transform(
          async (record) => {
            running++;
            running_max = Math.max(running_max, running);
            await new Promise((resolve) => setTimeout(resolve, 5));
            running--;
            return record;
          },
          { parallel: 4 },
        ),
      );
      running_max.should.eql(4);
      chunks.sort().should.eql([1, 2, 3, 4, 5, 6, 7, 8]);
    });

    it("parallel 1 serializes handlers", async function () {
      let running = 0;
      let running_max = 0;
      const chunks = await pipe(
        [1, 2, 3],
        transform(
          async (record) => {
            running++;
            running_max = Math.max(running_max, running);
            await new Promise((resolve) => setTimeout(resolve, 1));
            running--;
            return record;
          },
          { parallel: 1 },
        ),
      );
      running_max.should.eql(1);
      chunks.should.eql([1, 2, 3]);
    });
  });

  describe("errors", function () {
    it("error the stream when a synchronous handler throws", async function () {
      const err = await pipeError(
        ["a", "b"],
        transform((record) => {
          record;
          throw Error("Catchme");
        }),
      );
      err.message.should.eql("Catchme");
    });

    it("error the stream when a promise handler rejects", async function () {
      const err = await pipeError(
        ["a", "b"],
        transform(async (record) => {
          await new Promise((resolve) => setTimeout(resolve, 1));
          if (record === "b") throw Error("Catchme");
          return record;
        }),
      );
      err.message.should.eql("Catchme");
    });

    it("error the stream when a callback handler fails", async function () {
      const err = await pipeError(
        ["a", "b"],
        transform((record, callback) => {
          setTimeout(() => callback(Error("Catchme")), 1);
        }),
      );
      err.message.should.eql("Catchme");
    });

    it("error the stream while other handlers are still running", async function () {
      const err = await pipeError(
        [1, 2, 3, 4, 5],
        transform(
          async (record) => {
            await new Promise((resolve) =>
              setTimeout(resolve, record === 3 ? 1 : 20),
            );
            if (record === 3) throw Error("Catchme");
            return record;
          },
          { parallel: 5 },
        ),
      );
      err.message.should.eql("Catchme");
    });
  });

  describe("output parity with the Node.js implementation", function () {
    it("synchronous handler", async function () {
      const records = [
        ["a", 1],
        ["b", 2],
        ["c", 3],
      ];
      const handler = (record) => [...record].reverse();
      const chunks_web = await pipe(records, transform(handler));
      const chunks_node = await new Promise((resolve, reject) => {
        transformNode(records, handler, (err, data) => {
          if (err) reject(err);
          else resolve(data);
        });
      });
      chunks_web.should.eql(chunks_node);
    });

    it("callback handler", async function () {
      const records = ["a", "b", "c"];
      const handler = (record, callback) => {
        setImmediate(() => callback(null, record.toUpperCase()));
      };
      const chunks_web = await pipe(records, transform(handler));
      const chunks_node = await new Promise((resolve, reject) => {
        transformNode(records, handler, (err, data) => {
          if (err) reject(err);
          else resolve(data);
        });
      });
      chunks_web.should.eql(chunks_node);
    });
  });
});
