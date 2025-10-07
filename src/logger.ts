import * as log from "jsr:@std/log";

// Store the last log message and timestamp
let lastLog = { msg: "", time: 0 };

// Check for debug flag in arguments
const isDebug = Deno.args.includes("--debug");

// Configure the logger
await log.setup({
  handlers: {
    console: new log.ConsoleHandler(isDebug ? "DEBUG" : "INFO", {
      formatter: (record) => {
        // Check if this is a duplicate message within 500ms
        const now = Date.now();
        if (record.msg === lastLog.msg && now - lastLog.time < 500) {
          return ""; // Return empty string to skip logging
        }

        // Update last log info
        lastLog = { msg: record.msg, time: now };

        const time = new Date().toISOString();
        return `${time} [${record.levelName}] ${record.msg}`;
      },
    }),
  },
  loggers: {
    default: {
      level: isDebug ? "DEBUG" : "INFO",
      handlers: ["console"],
    },
  },
});

export { log }; 