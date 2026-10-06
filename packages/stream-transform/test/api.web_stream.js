import "should";
import { transform as transformNode } from "../lib/index.js";
import { transform } from "../lib/stream.js";

// Create a ReadableStream from a list of records
const records_stream = (records) =>
  new ReadableStream({
    start(controller) {
      for (const record of records) {
        controller.enqueue(record);
      }
      controller.close();
    },
  });

// Collect every record emitted by a Web TransformStream readable side
const collect = async (readable) => {
  const records = [];
  const reader = readable.getReader();
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    records.push(value);
  }
  return records;
};

const run_web = (records, handler, options) =>
  collect(records_stream(records).pipeThrough(transform(handler, options)));

// Run the same transformation through the Node.js stream implementation
const run_node = (records, handler, options) =>
  new Promise((resolve, reject) => {
    const transformer = transformNode(options, handler);
    const output = [];
    transformer.on("readable", function () {
      let record;
      while ((record = this.read()) !== null) {
        output.push(record);
      }
    });
    transformer.on("error", reject);
    transformer.on("end", () => resolve(output));
    for (const record of records) {
      transformer.write(record);
    }
    transformer.end();
  });

describe("api.web_stream", function () {
  it("returns a TransformStream", function () {
    transform((record) => record).should.be.instanceof(TransformStream);
  });

  it("supports the (options, handler) signature", async function () {
    const records = await run_web([1, 2, 3], { parallel: 2 }, (r) => r + 1);
    records.should.eql([2, 3, 4]);
  });

  it("runs synchronous handlers", async function () {
    const records = await run_web(
      [
        [1, 2],
        [3, 4],
      ],
      (record) => [...record, record[0] + record[1]],
    );
    records.should.eql([
      [1, 2, 3],
      [3, 4, 7],
    ]);
  });

  it("runs promise handlers", async function () {
    const records = await run_web([1, 2, 3], (record) =>
      Promise.resolve(record * 2),
    );
    records.should.eql([2, 4, 6]);
  });

  it("runs callback handlers", async function () {
    const records = await run_web([1, 2, 3], (record, callback) => {
      setImmediate(() => callback(null, record * 3));
    });
    records.should.eql(["3", "6", "9"]);
  });

  it("supports callback handlers with multiple outputs", async function () {
    const records = await run_web([1, 2], (record, callback) => {
      callback(null, `${record}a`, `${record + 10}b`);
    });
    records.should.eql(["1a", "11b", "2a", "12b"]);
  });

  it("coerces numbers emitted by callbacks to strings", async function () {
    const records = await run_web([1, 2], (record, callback) => {
      callback(null, record);
    });
    records.should.eql(["1", "2"]);
  });

  it("skips null and undefined returned records", async function () {
    const records = await run_web([1, 2, 3, 4], (record) =>
      record % 2 ? record : null,
    );
    records.should.eql([1, 3]);
  });

  it("skips null and undefined callback records and empty strings", async function () {
    const records = await run_web([0, 1, 2, 3], (record, callback) => {
      const outputs = [undefined, null, ""];
      setImmediate(() =>
        callback(null, record < outputs.length ? outputs[record] : `${record}`),
      );
    });
    records.should.eql(["3"]);
  });

  it("passes params to sync handlers", async function () {
    const seen = [];
    await run_web(
      [1, 2],
      (record, params) => {
        seen.push([record, params.multiplier]);
        return record * params.multiplier;
      },
      { params: { multiplier: 10 } },
    );
    seen.should.eql([
      [1, 10],
      [2, 10],
    ]);
  });

  it("passes params to callback handlers", async function () {
    const records = await run_web(
      [1, 2],
      (record, callback, params) => {
        setImmediate(() => callback(null, record * params.multiplier));
      },
      { params: { multiplier: 5 } },
    );
    records.should.eql(["5", "10"]);
  });

  it("exposes the state on the handler context", async function () {
    const states = [];
    await run_web(
      [1, 2, 3],
      function (record) {
        states.push({ ...this.state });
        return record;
      },
      { parallel: 1 },
    );
    states.should.eql([
      { running: 1, started: 1, finished: 0 },
      { running: 1, started: 2, finished: 1 },
      { running: 1, started: 3, finished: 2 },
    ]);
  });

  it("runs asynchronous handlers concurrently with parallel", async function () {
    let active = 0;
    let max_active = 0;
    const records = Array.from({ length: 20 }, (_, i) => i);
    // Resolved once the first batch of handlers is in flight
    let release;
    const gate = new Promise((resolve) => {
      release = resolve;
    });
    let filled = false;
    const stream = transform(
      (record) => {
        active++;
        max_active = Math.max(max_active, active);
        if (!filled && max_active === 5) {
          filled = true;
          release();
        }
        return gate.then(
          () =>
            new Promise((resolve) => {
              setTimeout(() => {
                active--;
                resolve(record);
              }, 5);
            }),
        );
      },
      { parallel: 5 },
    );
    const writer = stream.writable.getWriter();
    const reader = stream.readable.getReader();
    // Pull in the background so the writable side can deliver records
    const consume = (async () => {
      while (true) {
        const { done } = await reader.read();
        if (done) break;
      }
    })();
    for (const record of records) {
      writer.write(record);
    }
    writer.close();
    // Give the scheduler time to fill the 5 parallel slots
    await new Promise((resolve) => setTimeout(resolve, 30));
    filled.should.eql(true);
    await consume;
    max_active.should.eql(5);
  });

  it("does not queue records beyond parallel with parallel 1", async function () {
    let active = 0;
    let max_active = 0;
    await run_web(
      Array.from({ length: 10 }, (_, i) => i),
      (record) => {
        active++;
        max_active = Math.max(max_active, active);
        return new Promise((resolve) => {
          setTimeout(() => {
            active--;
            resolve(record);
          }, 5);
        });
      },
      { parallel: 1 },
    );
    max_active.should.eql(1);
  });

  it("ends the pipe with errors thrown by sync handlers", async function () {
    const stream = transform((record) => {
      throw new Error("Catchme " + record);
    });
    const writer = stream.writable.getWriter();
    const reader = stream.readable.getReader();
    await writer.write(1).should.be.rejectedWith("Catchme 1");
    await reader.read().should.be.rejectedWith("Catchme 1");
  });

  it("ends the pipe with rejected promises", async function () {
    const stream = transform((record) =>
      Promise.reject(new Error("Catchme " + record)),
    );
    const writer = stream.writable.getWriter();
    const reader = stream.readable.getReader();
    const write = writer.write(1);
    // The record is admitted before the promise rejects, but the readable
    // side errors and every subsequent write rejects with the same error.
    await reader.read().should.be.rejectedWith("Catchme 1");
    await writer.write(2).should.be.rejectedWith("Catchme 1");
    await write;
  });

  it("ends the pipe with callback errors", async function () {
    const stream = transform((record, callback) => {
      setImmediate(() => callback(new Error("Catchme")));
    });
    const writer = stream.writable.getWriter();
    const reader = stream.readable.getReader();
    const write = writer.write(1);
    await reader.read().should.be.rejectedWith("Catchme");
    await writer.write(2).should.be.rejectedWith("Catchme");
    await write;
  });

  it("does not hang when the error happens before flush", async function () {
    const stream = transform((record, callback) => {
      setImmediate(() => callback(new Error("Catchme")));
    });
    const writer = stream.writable.getWriter();
    await writer.write(1);
    // The close promise rejects once the stream terminates with the error,
    // the important contract is that it settles instead of hanging forever.
    await writer.close().should.be.rejected();
    await stream.readable.getReader().read().should.be.rejectedWith("Catchme");
  });

  it("closes cleanly with zero records", async function () {
    const records = await run_web([], (record) => record);
    records.should.eql([]);
  });

  it("throws on handlers with an invalid arity", async function () {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const stream = transform((a, b, c) => a);
    const writer = stream.writable.getWriter();
    await writer.write(1).should.be.rejectedWith("Invalid handler arguments");
  });

  it("emits records in completion order like the Node.js implementation", async function () {
    const records = await run_web(
      ["a", "b"],
      (record, callback) => {
        setTimeout(() => callback(null, record), record === "a" ? 50 : 5);
      },
      { parallel: 2 },
    );
    // "b" completes before "a", both implementations emit in completion order
    records.should.eql(["b", "a"]);
  });

  it("matches the Node.js implementation output", async function () {
    const input = Array.from({ length: 50 }, (_, i) => [i, `v${i}`]);
    const handler = (record, callback) => {
      setTimeout(
        () => callback(null, { id: record[0], value: record[1] }),
        1 + (record[0] % 3),
      );
    };
    const web_records = await run_web(input, handler, { parallel: 7 });
    const node_records = await run_node(input, handler, { parallel: 7 });
    // Both implementations emit records as handlers complete; with the same
    // handler and parallel option, they go through the same scheduling.
    web_records.sort((a, b) => a.id - b.id);
    node_records.sort((a, b) => a.id - b.id);
    web_records.should.eql(node_records);
  });
});
