import "should";
import { stringify, CsvError, Options } from "../lib/stream.js";

describe("API Web Stream Types", function () {
  it("expose a TransformStream of records to strings", async function () {
    const stream: TransformStream<unknown, string> = stringify();
    const writer = stream.writable.getWriter();
    await writer.write(["a", "b"]);
    await writer.close();
    const reader = stream.readable.getReader();
    const { done, value } = await reader.read();
    if (done !== false) throw Error("Expect a chunk");
    value.should.eql("a,b\n");
  });

  it("type the elements on both sides of pipeThrough", async function () {
    const source = new ReadableStream<[number, string]>({
      start(controller) {
        controller.enqueue([1, "a"]);
        controller.enqueue([2, "b"]);
        controller.close();
      },
    });
    const stream: ReadableStream<string> = source.pipeThrough(
      stringify<[number, string]>({ quoted: true }),
    );
    const reader = stream.getReader();
    const chunks: string[] = [];
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }
    chunks.join("").should.eql('"1","a"\n"2","b"\n');
  });

  it("accept the documented options", function () {
    const options: Options = {
      bom: true,
      cast: {
        boolean: (value) => (value ? "1" : ""),
        date: (value) => `${value.getTime()}`,
        number: (value) => `${value}`,
        bigint: (value) => `${value}`,
        object: (value) => JSON.stringify(value),
        string: (value) => value,
      },
      columns: ["a", { key: "b", header: "B" }],
      delimiter: ",",
      eof: true,
      escape: '"',
      header: true,
      header_as_comment: "#",
      quote: '"',
      quoted: true,
      quoted_empty: false,
      quoted_match: /,/,
      quoted_string: false,
      record_delimiter: "\n",
      escape_formulas: false,
    };
    stringify(options).should.be.an.Object();
  });

  it("expose CsvError", function () {
    const err = new CsvError("CSV_INVALID_ARGUMENT", ["some", "message"]);
    err.code.should.eql("CSV_INVALID_ARGUMENT");
    err.message.should.eql("some message");
  });
});
