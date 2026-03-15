import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import App, {
  extractError,
  formatLocations,
  formatPosted,
  getJson,
  normalizeDescription,
  normalizeCompany,
  postJson,
  chooseSelectedCompany,
  toCompanyOption,
  toTitleCase
} from "./App";

function makeResponse({ ok = true, status = 200, statusText = "OK", payload = {} } = {}) {
  return Promise.resolve({
    ok,
    status,
    statusText,
    json: async () => payload
  });
}

describe("App helpers", () => {
  beforeEach(() => {
    global.fetch = vi.fn();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("toTitleCase handles empty and separators", () => {
    expect(toTitleCase("")).toBe("");
    expect(toTitleCase("amazon_jobs-site")).toBe("Amazon Jobs Site");
  });

  it("toCompanyOption normalizes valid and invalid values", () => {
    expect(toCompanyOption(null)).toBeNull();
    expect(toCompanyOption("   ")).toBeNull();
    expect(toCompanyOption(" Amazon ")).toEqual({ value: "amazon", label: "Amazon" });
  });

  it("chooseSelectedCompany keeps existing selection when possible", () => {
    const options = [
      { value: "amazon", label: "Amazon" },
      { value: "google", label: "Google" }
    ];
    expect(chooseSelectedCompany("google", options)).toBe("google");
    expect(chooseSelectedCompany("meta", options)).toBe("amazon");
    expect(chooseSelectedCompany("", options)).toBe("amazon");
    expect(chooseSelectedCompany("google", [])).toBe("");
  });

  it("formatPosted handles missing and unix timestamp", () => {
    expect(formatPosted(null)).toBe("—");
    expect(formatPosted(1700000000)).toMatch(/\w{3}/);
  });

  it("formatLocations handles all branches", () => {
    expect(formatLocations(null)).toBe("—");
    expect(formatLocations([])).toBe("—");
    expect(formatLocations(["  "])).toBe("—");
    expect(formatLocations([123])).toBe("—");
    expect(formatLocations([{ city: 7, state: "WA", country: null }])).toBe("WA");
    expect(formatLocations(["Seattle"])).toBe("Seattle");
    expect(formatLocations([{ city: "Seattle", state: "WA", country: "US" }])).toBe("Seattle, WA, US");
    expect(
      formatLocations(
        ["A", "B", "C"],
        2
      )
    ).toBe("A • B +1 more");
  });

  it("normalizeDescription strips html and entities", () => {
    const input = "<p>Hello<br>World</p><li>One</li>&nbsp;&amp;";
    const output = normalizeDescription(input);
    expect(output).toContain("Hello");
    expect(output).toContain("World");
    expect(output).toContain("• One");
    expect(output).toContain("&");
    expect(normalizeDescription("")).toBe("");
  });

  it("normalizeCompany handles nullish and trimming", () => {
    expect(normalizeCompany(undefined)).toBe("");
    expect(normalizeCompany(null)).toBe("");
    expect(normalizeCompany(" amazon ")).toBe("amazon");
  });

  it("extractError handles detail string/object/fallback", async () => {
    await expect(
      extractError({
        status: 500,
        statusText: "ERR",
        json: async () => ({ detail: "bad" })
      })
    ).resolves.toBe("bad");

    await expect(
      extractError({
        status: 400,
        statusText: "Bad",
        json: async () => ({ detail: { code: "X" } })
      })
    ).resolves.toBe('{"code":"X"}');

    await expect(
      extractError({
        status: 404,
        statusText: "Not Found",
        json: async () => ({})
      })
    ).resolves.toBe("404 Not Found");

    await expect(
      extractError({
        status: 503,
        statusText: "Service Unavailable",
        json: async () => {
          throw new Error("boom");
        }
      })
    ).resolves.toBe("503 Service Unavailable");
  });

  it("postJson and getJson handle ok and error", async () => {
    fetch.mockImplementationOnce(() => makeResponse({ ok: true, payload: { a: 1 } }));
    await expect(postJson("/x", { q: 1 })).resolves.toEqual({ a: 1 });

    fetch.mockImplementationOnce(() =>
      makeResponse({ ok: false, status: 502, statusText: "Bad Gateway", payload: { detail: "upstream" } })
    );
    await expect(postJson("/x", { q: 1 })).rejects.toThrow("upstream");

    fetch.mockImplementationOnce(() => makeResponse({ ok: true, payload: { b: 2 } }));
    await expect(getJson("/y")).resolves.toEqual({ b: 2 });

    fetch.mockImplementationOnce(() =>
      makeResponse({ ok: false, status: 500, statusText: "ERR", payload: { detail: "down" } })
    );
    await expect(getJson("/y")).rejects.toThrow("down");
  });
});

describe("App component", () => {
  beforeEach(() => {
    global.fetch = vi.fn();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("loads companies and initial jobs then opens/caches modal details", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [
              {
                id: "job-1",
                name: "Software Engineer",
                company: "amazon",
                locations: [{ city: "Seattle", state: "WA", country: "US" }],
                postedTs: 1700000000,
                detailsUrl: "https://example.com/details",
                applyUrl: "https://example.com/apply"
              },
              {
                name: "No Id",
                company: "amazon",
                locations: []
              }
            ],
            total_results: 2,
            page_size: 1,
            pagination_index: 1,
            has_next_page: true
          }
        })
      )
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobDescription: "<p>Hello<br>World</p>"
          }
        })
      );

    render(<App />);

    expect(await screen.findByText("Software Engineer")).toBeInTheDocument();
    expect(screen.getByText("Page 1 of 2")).toBeInTheDocument();

    fireEvent.click(screen.getByText("No Id"));
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));

    fireEvent.click(screen.getByText("Software Engineer"));

    expect(await screen.findByRole("button", { name: "Close" })).toBeInTheDocument();
    expect(await screen.findByText("Job Description")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "View details page" })).toHaveAttribute(
      "href",
      "https://example.com/details"
    );
    expect(screen.getByRole("link", { name: "Apply now" })).toHaveAttribute("href", "https://example.com/apply");

    fireEvent.keyDown(window, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("button", { name: "Close" })).not.toBeInTheDocument());

    fireEvent.click(screen.getByText("Software Engineer"));
    expect(await screen.findByRole("button", { name: "Close" })).toBeInTheDocument();
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(3));

    fireEvent.click(screen.getByRole("button", { name: "Close" }));
    await waitFor(() => expect(screen.queryByRole("button", { name: "Close" })).not.toBeInTheDocument());
  });

  it("shows company-load fallback errors and empty companies state", async () => {
    fetch.mockImplementationOnce(() => makeResponse({ payload: {} }));

    render(<App />);

    expect(await screen.findByText("No companies returned by API.")).toBeInTheDocument();
    expect(screen.getByText("No jobs found on this page.")).toBeInTheDocument();
    expect(screen.getByText("No companies")).toBeInTheDocument();
  });

  it("shows default company-load error message for non-Error failures", async () => {
    fetch.mockImplementationOnce(() => Promise.reject("network-down"));
    render(<App />);
    expect(await screen.findByText("Failed to load companies.")).toBeInTheDocument();
  });

  it("handles details error response and payload-without-job", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [{ id: "role-1", name: "Role One", company: "amazon", locations: [] }],
            total_results: 1,
            pagination_index: 1,
            has_next_page: false
          }
        })
      )
      .mockImplementationOnce(() =>
        makeResponse({ ok: false, status: 503, statusText: "Service Unavailable", payload: { detail: "blocked" } })
      )
      .mockImplementationOnce(() =>
        makeResponse({ payload: { error: "missing job" } })
      );

    render(<App />);

    expect(await screen.findByText("Role One")).toBeInTheDocument();
    fireEvent.click(screen.getByText("Role One"));
    expect(await screen.findByText("blocked")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Close" }));

    fireEvent.click(screen.getByText("Role One"));
    expect(await screen.findByText("missing job")).toBeInTheDocument();
  });

  it("handles details payload-without-job and non-string error fallback", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [{ id: "role-x", name: "Role X", company: "amazon", locations: [] }],
            total_results: 1,
            pagination_index: 1,
            has_next_page: false
          }
        })
      )
      .mockImplementationOnce(() => makeResponse({ payload: { error: { code: "MISSING" } } }));

    render(<App />);
    expect(await screen.findByText("Role X")).toBeInTheDocument();
    fireEvent.click(screen.getByText("Role X"));
    expect(await screen.findByText("Failed to load details.")).toBeInTheDocument();
  });

  it("shows open-posting fallback action when apply link is missing", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [
              {
                id: "role-open",
                name: "Role Open",
                company: "amazon",
                locations: [],
                applyUrl: ""
              }
            ],
            total_results: 1,
            pagination_index: 1,
            has_next_page: false
          }
        })
      )
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobDescription: "Details only",
            detailsUrl: "https://example.com/details-only"
          }
        })
      );

    render(<App />);

    expect(await screen.findByText("Role Open")).toBeInTheDocument();
    fireEvent.click(screen.getByText("Role Open"));
    expect(await screen.findByRole("link", { name: "View details page" })).toHaveAttribute(
      "href",
      "https://example.com/details-only"
    );
  });

  it("aborts in-flight details request when modal closes", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [{ id: "role-abort", name: "Role Abort", company: "amazon", locations: [] }],
            total_results: 1,
            pagination_index: 1,
            has_next_page: false
          }
        })
      )
      .mockImplementationOnce((_url, options) =>
        new Promise((_resolve, reject) => {
          options.signal.addEventListener("abort", () => {
            const aborted = new Error("aborted");
            aborted.name = "AbortError";
            reject(aborted);
          });
        })
      );

    render(<App />);

    expect(await screen.findByText("Role Abort")).toBeInTheDocument();
    fireEvent.click(screen.getByText("Role Abort"));
    expect(await screen.findByText("Loading details...")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Close" }));
    await waitFor(() => expect(screen.queryByRole("button", { name: "Close" })).not.toBeInTheDocument());
  });

  it("keeps modal open on non-escape keydown and handles non-Error details failure", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [{ id: "role-y", company: "amazon", locations: [] }],
            total_results: 1,
            pagination_index: 1,
            has_next_page: false
          }
        })
      )
      .mockImplementationOnce(() => Promise.reject("details-down"));

    render(<App />);
    expect(await screen.findByText("Positions")).toBeInTheDocument();
    await waitFor(() => expect(document.querySelector("tr.row-clickable")).not.toBeNull());
    const row = document.querySelector("tr.row-clickable");
    fireEvent.click(row);
    expect(await screen.findByText("Failed to load details.")).toBeInTheDocument();

    fireEvent.keyDown(window, { key: "Enter" });
    expect(screen.getByRole("button", { name: "Close" })).toBeInTheDocument();
  });

  it("supports next and previous pagination and company switching", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon", "google"] } }))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [{ id: "a-1", name: "A1", company: "amazon", locations: [] }],
            total_results: 2,
            page_size: 1,
            pagination_index: 1,
            has_next_page: true
          }
        })
      )
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [{ id: "a-2", name: "A2", company: "amazon", locations: [] }],
            total_results: 2,
            page_size: 1,
            pagination_index: 2,
            has_next_page: false
          }
        })
      )
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [{ id: "a-1", name: "A1", company: "amazon", locations: [] }],
            total_results: 2,
            page_size: 1,
            pagination_index: 1,
            has_next_page: true
          }
        })
      )
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [{ id: "9", name: "G1", company: "google", locations: [] }],
            total_results: 1,
            page_size: 25,
            pagination_index: 1,
            has_next_page: false
          }
        })
      );

    render(<App />);

    expect(await screen.findByText("A1")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    expect(await screen.findByText("A2")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Previous" }));
    expect(await screen.findByText("A1")).toBeInTheDocument();

    fireEvent.change(screen.getByRole("combobox"), { target: { value: "google" } });
    expect(await screen.findByText("G1")).toBeInTheDocument();
  });

  it("handles pagination-index fallback and search error reset", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [{ id: "role-1", name: "Role One", company: "amazon", locations: [] }],
            total_results: 1,
            page_size: 1,
            total_pages: 9,
            has_next_page: true,
            pagination_index: "not-a-number"
          }
        })
      )
      .mockImplementationOnce(() =>
        makeResponse({ ok: false, status: 500, statusText: "ERR", payload: { detail: "boom" } })
      );

    render(<App />);

    expect(await screen.findByText("Role One")).toBeInTheDocument();
    expect(screen.getByText("Page 1 of 9")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    await waitFor(() => expect(screen.getByText("No jobs found on this page.")).toBeInTheDocument());
    expect(screen.getByText("boom")).toBeInTheDocument();
    expect(screen.getByText("Page 1")).toBeInTheDocument();
  });

  it("handles missing jobs payload fallback values", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            total_results: "unknown",
            page_size: 0,
            has_next_page: false,
            pagination_index: 1
          }
        })
      );

    render(<App />);
    expect(await screen.findByText("No jobs found on this page.")).toBeInTheDocument();
    expect(screen.getByText("Page 1")).toBeInTheDocument();
  });

  it("handles non-Error search failure with default message", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() => Promise.reject("jobs-down"));

    render(<App />);
    expect(await screen.findByText("Search request failed.")).toBeInTheDocument();
  });

  it("aborts previous details request when opening another row and falls back to Company label", async () => {
    let firstDetailsSignal;
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [
              { id: "role-1", name: "Role One", company: "amazon", locations: [] },
              { id: "role-2", name: "Role Two", locations: [] }
            ],
            total_results: 2,
            page_size: 2,
            pagination_index: 1,
            has_next_page: false
          }
        })
      )
      .mockImplementationOnce((_url, options) => {
        firstDetailsSignal = options.signal;
        return new Promise((_resolve, reject) => {
          options.signal.addEventListener("abort", () => {
            const aborted = new Error("aborted");
            aborted.name = "AbortError";
            reject(aborted);
          });
        });
      })
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobDescription: "Second role details"
          }
        })
      );

    render(<App />);

    expect(await screen.findByText("Role One")).toBeInTheDocument();
    fireEvent.click(screen.getByText("Role One"));
    await screen.findByText("Loading details...");
    fireEvent.click(screen.getByText("Role Two"));
    await waitFor(() => expect(firstDetailsSignal.aborted).toBe(true));
    await screen.findByRole("button", { name: "Close" });
    const detailsCompanyLabel = document.querySelector(".details-meta span");
    expect(detailsCompanyLabel?.textContent).toBe("Company");
  });
});
