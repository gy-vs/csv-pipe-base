import "should";
import { stringify as stringifyNode } from "../lib/index.js";
import { stringify } from "../lib/stream.js";

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

// Collect every chunk emitted by a Web TransformStream readable side
const collect_web = async (readable) => {
  const chunks = [];
  const reader = readable.getReader();
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
  return chunks;
};

// Collect the output of the Node.js stringifier with the same records/options
const collect_node = (records, options) =>
  new Promise((resolve, reject) => {
    const stringifier = stringifyNode(options);
    const chunks = [];
    stringifier.on("readable", function () {
      let chunk;
      while ((chunk = this.read()) !== null) {
        chunks.push(chunk);
      }
    });
    stringifier.on("error", reject);
    stringifier.on("end", () => resolve(chunks));
    for (const record of records) {
      stringifier.write(record);
    }
    stringifier.end();
  });

// Compare the web stream output, byte for byte, with the Node.js output
const should_match = async (records, options) => {
  const node_chunks = await collect_node(records, options);
  const web_chunks = await collect_web(
    records_stream(records).pipeThrough(stringify(options)),
  );
  const node_bytes = Buffer.concat(node_chunks.map((c) => Buffer.from(c)));
  const web_bytes = Buffer.concat(web_chunks.map((c) => Buffer.from(c)));
  web_bytes.should.eql(node_bytes);
  return web_bytes;
};

describe("api.web_stream", function () {
  it("returns a TransformStream", function () {
    const stream = stringify();
    stream.should.be.instanceof(TransformStream);
  });

  it("stringify arrays", async function () {
    const bytes = await should_match(
      [
        ["a", "b", "c"],
        ["1", "2", "3"],
      ],
      {},
    );
    bytes.toString().should.eql("a,b,c\n1,2,3\n");
  });

  it("stringify objects with auto discovered columns", async function () {
    const records = [
      { a: "1", b: "2" },
      { a: "3", b: "4" },
    ];
    const bytes = await should_match(records, {});
    bytes.toString().should.eql("1,2\n3,4\n");
  });

  it("header with columns", async function () {
    const bytes = await should_match(
      [
        { a: "1", b: "2" },
        { a: "3", b: "4" },
      ],
      { header: true, columns: ["a", "b"] },
    );
    bytes.toString().should.eql("a,b\n1,2\n3,4\n");
  });

  it("header with zero records emits the header", async function () {
    const bytes = await should_match([], {
      header: true,
      columns: ["a", "b"],
    });
    bytes.toString().should.eql("a,b\n");
  });

  it("zero records without header emits nothing", async function () {
    const bytes = await should_match([], {});
    bytes.length.should.eql(0);
  });

  it("bom with records", async function () {
    const bytes = await should_match([["a"]], { bom: true });
    [...bytes].should.eql([239, 187, 191, 97, 10]);
  });

  it("bom with header and zero records", async function () {
    const bytes = await should_match([], {
      bom: true,
      header: true,
      columns: ["a"],
    });
    [...bytes].should.eql([239, 187, 191, 97, 10]);
  });

  it("bom emits a binary chunk", async function () {
    const chunks = await collect_web(
      records_stream([["a"]]).pipeThrough(stringify({ bom: true })),
    );
    chunks[0].should.be.instanceof(Uint8Array);
    [...chunks[0]].should.eql([239, 187, 191]);
    Buffer.from(chunks[1]).toString().should.eql("a\n");
  });

  it("always emits binary chunks consumable by a Response", async function () {
    const stream = records_stream([
      ["a", "b"],
      ["c", "d"],
    ]).pipeThrough(stringify());
    const response = new Response(stream);
    (await response.text()).should.eql("a,b\nc,d\n");
  });

  it("custom delimiter and record_delimiter", async function () {
    const bytes = await should_match(
      [
        ["a", "b"],
        ["c", "d"],
      ],
      { delimiter: "|", record_delimiter: "windows" },
    );
    bytes.toString().should.eql("a|b\r\nc|d\r\n");
  });

  it("quoted fields", async function () {
    const bytes = await should_match(
      [
        ["a,b", 'c"d'],
        ["e\nf", "g"],
      ],
      { quoted: true },
    );
    bytes.toString().should.eql('"a,b","c""d"\n"e\nf","g"\n');
  });

  it("eof false", async function () {
    const bytes = await should_match(
      [
        ["a", "b"],
        ["c", "d"],
      ],
      { eof: false },
    );
    bytes.toString().should.eql("a,b\nc,d");
  });

  it("eof false with zero records and header", async function () {
    const bytes = await should_match([], {
      eof: false,
      header: true,
      columns: ["a", "b"],
    });
    bytes.toString().should.eql("a,b");
  });

  it("custom cast", async function () {
    const bytes = await should_match([[true, 1, new Date(0)]], {
      cast: {
        boolean: (value) => (value ? "Y" : "N"),
        number: (value) => `n:${value}`,
        date: () => "epoch",
      },
    });
    bytes.toString().should.eql("Y,n:1,epoch\n");
  });

  it("columns option reorders object fields", async function () {
    const bytes = await should_match([{ a: "1", b: "2", c: "3" }], {
      header: true,
      columns: ["c", "a"],
    });
    bytes.toString().should.eql("c,a\n3,1\n");
  });

  it("matches the Node.js output across option combinations", async function () {
    const datasets = [
      [],
      [["a", "b"]],
      [
        ["simple", "with,comma"],
        ['with"quote', "with\nnewline"],
      ],
      [
        { name: "a", tag: "x,y" },
        { name: "b", tag: 'q"z' },
      ],
      [
        ["utf8", "Ω≈ç√"],
        ["formula", "=1+1"],
      ],
    ];
    const option_sets = [
      {},
      { header: true, columns: undefined },
      { bom: true },
      { header: true, bom: true, columns: ["name", "tag"] },
      { delimiter: ";", record_delimiter: "windows" },
      { quoted: true },
      { quoted_string: true, quoted_empty: true },
      { eof: false },
      { eof: false, header: true, columns: ["f1", "f2"] },
      { quote: "'", escape: "\\" },
      { escape_formulas: true },
      {
        cast: {
          boolean: (v) => (v ? "yes" : "no"),
          number: (v) => `#${v}`,
        },
      },
    ];
    for (const records of datasets) {
      for (const options of option_sets) {
        // Skip option/records combinations which both implementations reject
        const valid =
          !options.header ||
          options.columns !== undefined ||
          (records.length > 0 && !Array.isArray(records[0]));
        if (!valid) continue;
        if (options.columns && records.some((r) => Array.isArray(r))) {
          // Array records ignore named columns, skip meaningless combos
          continue;
        }
        await should_match(records, options);
      }
    }
  });

  it("surfaces stringification errors", async function () {
    const stream = stringify({ header: true });
    const reader = stream.readable.getReader();
    const writer = stream.writable.getWriter();
    await writer.write(["a"]);
    const write_promise = writer.write(["b"]);
    await reader.read().should.be.rejected();
    await write_promise.should.be.rejectedWith(/Undiscoverable Columns/);
  });

  it("rejects invalid options at construction", function () {
    (() => stringify({ bom: "yes" })).should.throw();
  });
});
