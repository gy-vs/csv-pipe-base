class CsvError extends Error {
  constructor(code, message, ...contexts) {
    if (Array.isArray(message)) message = message.join(" ");
    super(message);
    if (Error.captureStackTrace !== undefined) {
      Error.captureStackTrace(this, CsvError);
    }
    this.code = code;
    for (const context of contexts) {
      for (const key in context) {
        const value = context[key];
        this[key] =
          // `Buffer` is not available in every JavaScript environment,
          // such as edge runtimes.
          typeof Buffer !== "undefined" && Buffer.isBuffer(value)
            ? value.toString()
            : value == null
              ? value
              : JSON.parse(JSON.stringify(value));
      }
    }
  }
}

export { CsvError };
