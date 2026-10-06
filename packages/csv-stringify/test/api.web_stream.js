import "should";
import { stringify } from "../lib/index.js";
import { stringify as stringifyStream } from "../lib/stream.js";

// Run records through the Web Streams API and return the concatenated output
const stringifyWeb = async (records, options) => {
  const stream = stringifyStream(options);
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
  return chunks.join("");
};

// Run the same records through the Node.js stream API
const stringifyNode = (records, options) =>
  new Promise((resolve, reject) => {
    stringify(records, options, (err, output) => {
      if (err) reject(err);
      else resolve(output);
    });
  });

describe("API Web Stream", function () {
  it("emit strings of CSV text", async function () {
    const stream = stringifyStream();
    const writer = stream.writable.getWriter();
    const reader = stream.readable.getReader();
    await writer.write(["a", "b"]);
    await writer.write(["c", "d"]);
    await writer.close();
    const chunk_1 = await reader.read();
    chunk_1.should.eql({ done: false, value: "a,b\n" });
    const chunk_2 = await reader.read();
    chunk_2.should.eql({ done: false, value: "c,d\n" });
    const chunk_3 = await reader.read();
    chunk_3.should.eql({ done: true, value: undefined });
  });

  it("discover columns from object records and print the header", async function () {
    const output = await stringifyWeb(
      [
        { a: "1", b: "2" },
        { a: "3", b: "4" },
      ],
      { header: true },
    );
    output.should.eql("a,b\n1,2\n3,4\n");
  });

  describe("output parity with the Node.js implementation", function () {
    // Each case must produce the exact same output as the Node.js
    // implementation, including the BOM and the header when no record
    // is written.
    const cases = [
      {
        name: "default options",
        records: [
          [1, "a", true, null],
          [2, "b", false, undefined],
        ],
        options: {},
      },
      {
        name: "fields requiring quotes and escapes",
        records: [
          ['a"b', "c\nd", "e,f"],
          ["", " ", "g"],
        ],
        options: {},
      },
      {
        name: "object records with header",
        records: [
          { a: 1, b: "x" },
          { a: 2, b: "y" },
        ],
        options: { header: true },
      },
      {
        name: "columns with custom header labels",
        records: [
          { a: 1, b: "x", c: "ignored" },
          { a: 2, b: "y", c: "ignored" },
        ],
        options: {
          header: true,
          columns: [
            { key: "a", header: "A" },
            { key: "b", header: "B" },
          ],
        },
      },
      {
        name: "columns as an object map",
        records: [
          { a: 1, b: 2 },
          { a: 3, b: 4 },
        ],
        options: { header: true, columns: { b: "B", a: "A" } },
      },
      {
        name: "bom",
        records: [["a"], ["b"]],
        options: { bom: true },
      },
      {
        name: "bom with header",
        records: [{ a: 1 }, { a: 2 }],
        options: { bom: true, header: true },
      },
      {
        name: "quoted",
        records: [
          [1, "a", ""],
          [2, "b", "c"],
        ],
        options: { quoted: true },
      },
      {
        name: "quoted_string",
        records: [
          [1, "a"],
          [2, "b"],
        ],
        options: { quoted_string: true },
      },
      {
        name: "quoted_empty",
        records: [
          ["", "a"],
          ["b", ""],
        ],
        options: { quoted_empty: true },
      },
      {
        name: "quoted_match",
        records: [
          ["abc", "def"],
          ["ghi", "jkl"],
        ],
        options: { quoted_match: /c|j/ },
      },
      {
        name: "delimiter",
        records: [
          ["a", "b"],
          ["c", "d"],
        ],
        options: { delimiter: ";" },
      },
      {
        name: "record_delimiter windows",
        records: [
          ["a", "b"],
          ["c", "d"],
        ],
        options: { record_delimiter: "windows" },
      },
      {
        name: "record_delimiter with multiple characters",
        records: [
          ["a", "b"],
          ["c", "d"],
        ],
        options: { record_delimiter: "||" },
      },
      {
        name: "eof false",
        records: [
          ["a", "b"],
          ["c", "d"],
        ],
        options: { eof: false },
      },
      {
        name: "cast",
        records: [
          [1, true, new Date(0)],
          [2, false, new Date(1000)],
        ],
        options: {
          cast: {
            boolean: (value) => (value ? "yes" : "no"),
            date: (value) => `ts:${value.getTime()}`,
            number: (value) => `#${value}`,
          },
        },
      },
      {
        name: "escape_formulas",
        records: [["=1+1"], ["+sum"], ["plain"]],
        options: { escape_formulas: true },
      },
      {
        name: "no records",
        records: [],
        options: {},
      },
      {
        name: "no records with bom",
        records: [],
        options: { bom: true },
      },
      {
        name: "no records with header and columns",
        records: [],
        options: { header: true, columns: ["a", "b"] },
      },
      {
        name: "no records with bom, header and columns",
        records: [],
        options: { bom: true, header: true, columns: ["a", "b"] },
      },
    ];
    for (const { name, records, options } of cases) {
      it(name, async function () {
        const output_node = await stringifyNode(records, options);
        const output_web = await stringifyWeb(records, options);
        output_web.should.eql(output_node);
        // Compare the outputs byte by byte
        Buffer.from(output_web).should.eql(Buffer.from(output_node));
      });
    }
  });

  describe("errors", function () {
    it("error the stream on invalid records", async function () {
      const stream = stringifyStream();
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      // A string is not a valid record, expect an array or an object
      await writer.write("invalid").catch(() => {});
      try {
        await reader.read();
        throw Error("Shall not be called");
      } catch (err) {
        err.message.should.eql(
          'Invalid Record: expect an array or an object, got "invalid"',
        );
      }
      // The writable side is errored as well
      try {
        await writer.write(["a"]);
        throw Error("Shall not be called");
      } catch (err) {
        err.message.should.eql(
          'Invalid Record: expect an array or an object, got "invalid"',
        );
      }
    });

    it("error with header and array records", async function () {
      const stream = stringifyStream({ header: true });
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      await writer.write(["a", "b"]).catch(() => {});
      try {
        await reader.read();
        throw Error("Shall not be called");
      } catch (err) {
        err.message.should.eql(
          "Undiscoverable Columns: header option requires column option or object records",
        );
      }
      try {
        await writer.write(["c", "d"]);
        throw Error("Shall not be called");
      } catch (err) {
        err.message.should.eql(
          "Undiscoverable Columns: header option requires column option or object records",
        );
      }
    });

    it("error when the cast function throws", async function () {
      const stream = stringifyStream({
        cast: {
          string: () => {
            throw Error("Catchme");
          },
        },
      });
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      await writer.write(["a"]).catch(() => {});
      try {
        await reader.read();
        throw Error("Shall not be called");
      } catch (err) {
        err.message.should.eql("Catchme");
      }
      try {
        await writer.write(["b"]);
        throw Error("Shall not be called");
      } catch (err) {
        err.message.should.eql("Catchme");
      }
    });
  });
});
