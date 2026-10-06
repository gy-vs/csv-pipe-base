import "should";
import { stringify } from "../lib/stream.js";
import { Options } from "../lib/stream.js";

describe("api.types.web_stream", function () {
  it("infers record types through pipeThrough", function () {
    const source: ReadableStream<Array<string | number>> = new ReadableStream({
      start(controller) {
        controller.enqueue(["a", 1]);
        controller.close();
      },
    });
    const options: Options = { header: false };
    const output = source.pipeThrough(
      stringify<Array<string | number>>(options),
    );
    // ReadableStream is parameterized with the output chunk type
    const reader = output.getReader();
    const check: Promise<ReadableStreamReadResult<Uint8Array>> = reader.read();
    check.should.be.a.Promise();
  });

  it("accepts object records", function () {
    const stream: TransformStream<{ a: string }, Uint8Array> =
      stringify<{ a: string }>({ header: true, columns: ["a"] });
    stream.writable.getWriter().write({ a: "1" });
  });

  it("defaults the input type to the Input array type", function () {
    const stream = stringify();
    // Input is `unknown[]`, so the writable expects array records
    const writer: WritableStreamDefaultWriter<unknown[]> =
      stream.writable.getWriter();
    writer.write(["a", "b"]);
  });
});
