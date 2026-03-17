import "@testing-library/jest-dom/vitest";
import { afterEach, beforeEach, vi } from "vitest";

let consoleErrorSpy;
let consoleWarnSpy;
let consoleMessages = [];

function formatConsoleArgs(args) {
  return args
    .map((value) => {
      if (typeof value === "string") {
        return value;
      }
      try {
        return JSON.stringify(value);
      } catch {
        return String(value);
      }
    })
    .join(" ");
}

beforeEach(() => {
  consoleMessages = [];
  consoleErrorSpy = vi.spyOn(console, "error").mockImplementation((...args) => {
    consoleMessages.push(`console.error: ${formatConsoleArgs(args)}`);
  });
  consoleWarnSpy = vi.spyOn(console, "warn").mockImplementation((...args) => {
    consoleMessages.push(`console.warn: ${formatConsoleArgs(args)}`);
  });
});

afterEach(() => {
  consoleErrorSpy.mockRestore();
  consoleWarnSpy.mockRestore();
  if (consoleMessages.length > 0) {
    throw new Error(`Unexpected frontend warning output:\n${consoleMessages.join("\n")}`);
  }
});
