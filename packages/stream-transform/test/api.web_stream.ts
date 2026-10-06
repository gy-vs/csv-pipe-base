import "should";
import {
  transform,
  Options,
  HandlerContext,
  SyncHandler,
  CallbackHandler,
} from "../lib/stream.js";

describe("api.types.web_stream", function () {
  it("infers output type from a sync handler", function () {
    const source: ReadableStream<number> = new ReadableStream({
      start(controller) {
        controller.enqueue(1);
        controller.close();
      },
    });
    const output = source.pipeThrough(
      transform((record: number) => `n${record}`),
    );
    const reader = output.getReader();
    const check: Promise<ReadableStreamReadResult<string>> = reader.read();
    check.should.be.a.Promise();
  });

  it("infers output type from a promise handler", function () {
    const stream = transform((record: number) =>
      Promise.resolve({ id: record }),
    );
    const output: ReadableStream<{ id: number }> = stream.readable;
    output.locked.should.eql(false);
  });

  it("infers output type from a callback handler", function () {
    const handler: CallbackHandler<number, number> = (record, callback) => {
      callback(null, record + 1);
    };
    const stream = transform(handler);
    const output: ReadableStream<number> = stream.readable;
    output.locked.should.eql(false);
  });

  it("removes null and undefined from the sync output type", function () {
    const stream = transform((record: number) => (record % 2 ? record : null));
    const output: ReadableStream<number> = stream.readable;
    output.locked.should.eql(false);
  });

  it("accepts an options first argument", function () {
    const options: Options = { parallel: 4, params: { key: "value" } };
    const stream: TransformStream<number, string> = transform(
      options,
      (record: number) => `${record}`,
    );
    stream.should.be.instanceof(TransformStream);
  });

  it("accepts a typed sync handler", function () {
    const handler: SyncHandler<number, string> = function (record) {
      const running: number = this.state.running;
      const params: unknown = this.options.params;
      return `${record}:${running}:${params}`;
    };
    const stream = transform(handler, { parallel: 2 });
    stream.should.be.instanceof(TransformStream);
  });

  it("accepts a typed callback handler", function () {
    const handler: CallbackHandler<number, boolean> = function (
      record,
      callback,
    ) {
      callback(null, record > 0);
    };
    const stream: TransformStream<number, boolean> = transform(handler);
    stream.should.be.instanceof(TransformStream);
  });

  it("exposes the HandlerContext type", function () {
    const assert_context = (ctx: HandlerContext) => {
      ctx.state.running.should.eql(0);
      ctx.state.started.should.eql(0);
      ctx.state.finished.should.eql(0);
    };
    transform<number, number>(
      function (record) {
        assert_context(this);
        return record;
      },
      { parallel: 1 },
    );
  });
});
