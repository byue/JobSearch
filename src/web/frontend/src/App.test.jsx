import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import App, {
  extractError,
  formatLocations,
  formatPosted,
  getJson,
  normalizeDescription,
  normalizeCompany,
  normalizeJobType,
  normalizeLocationFilter,
  normalizePostedWithin,
  formatLocationFilterSummary,
  retainSelectedOption,
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

const LOCATION_FILTERS_PAYLOAD = { payload: { countries: [], regions: [], cities: [] } };

function getCallsMatching(path) {
  return fetch.mock.calls.filter(([url]) => String(url).includes(path));
}

function getLastPostedBody(path) {
  const calls = getCallsMatching(path);
  const lastCall = calls.at(-1);
  if (!lastCall?.[1]?.body) {
    return null;
  }
  return JSON.parse(lastCall[1].body);
}

function getReactProps(element) {
  const reactPropsKey = Object.keys(element).find((key) => key.startsWith("__reactProps"));
  return reactPropsKey ? element[reactPropsKey] : null;
}

async function openResultsPopup(expectedJobRequests = 1) {
  await waitFor(() => expect(screen.getByLabelText("Company")).toHaveValue("__all__"));
  await waitFor(() => expect(getCallsMatching("/get_location_filters").length).toBeGreaterThanOrEqual(1));
  fireEvent.click(screen.getByRole("button", { name: "Search" }));
  await waitFor(() => expect(getCallsMatching("/get_jobs").length).toBe(expectedJobRequests));
  await screen.findByRole("dialog", { name: "Search results" });
  await waitFor(() => expect(screen.queryByText("Loading positions...")).not.toBeInTheDocument());
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

  it("normalizePostedWithin accepts supported windows only", () => {
    expect(normalizePostedWithin(undefined)).toBe("");
    expect(normalizePostedWithin(" 7d ")).toBe("7d");
    expect(normalizePostedWithin("bad")).toBe("");
  });

  it("normalizeJobType accepts supported values only", () => {
    expect(normalizeJobType(undefined)).toBe("");
    expect(normalizeJobType("software_engineer")).toBe("software_engineer");
    expect(normalizeJobType(" Machine_Learning_Engineer ")).toBe("machine_learning_engineer");
    expect(normalizeJobType("bad")).toBe("");
  });

  it("normalizeLocationFilter trims values", () => {
    expect(normalizeLocationFilter(undefined)).toBe("");
    expect(normalizeLocationFilter(" Seattle ")).toBe("Seattle");
  });

  it("retainSelectedOption keeps only values that still exist", () => {
    expect(retainSelectedOption("Seattle", ["Seattle", "Tacoma"])).toBe("Seattle");
    expect(retainSelectedOption("Portland", ["Seattle", "Tacoma"])).toBe("");
    expect(retainSelectedOption("", ["Seattle"])).toBe("");
  });

  it("formatLocationFilterSummary prefers city-first summary", () => {
    expect(formatLocationFilterSummary("", "", "")).toBe("Any location");
    expect(formatLocationFilterSummary("United States", "", "")).toBe("United States");
    expect(formatLocationFilterSummary("United States", "Washington", "Seattle")).toBe(
      "Seattle, Washington, United States"
    );
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
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [
              {
                id: "job-1",
                runId: "run-1",
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
            jobDescription: "<p>Hello<br>World</p>",
            skills: ["Python", "SQL"]
          }
        })
      );

    render(<App />);
    await openResultsPopup();

    expect(await screen.findByText("Software Engineer", { selector: "h3" })).toBeInTheDocument();
    expect(screen.getByText("Page 1 of 2")).toBeInTheDocument();

    fireEvent.click(screen.getByText("No Id"));
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(3));

    fireEvent.click(screen.getByText("Software Engineer", { selector: "h3" }));

    expect(await screen.findByRole("button", { name: "Close" })).toBeInTheDocument();
    expect(await screen.findByText("Job Description")).toBeInTheDocument();
    expect(screen.getByText("Skills")).toBeInTheDocument();
    expect(screen.getByText("Python")).toBeInTheDocument();
    expect(screen.getByText("SQL")).toBeInTheDocument();
    expect(getLastPostedBody("/get_job_details")).toEqual({
      job_id: "job-1",
      company: "amazon",
      runId: "run-1"
    });
    expect(screen.getByRole("link", { name: "View details page" })).toHaveAttribute(
      "href",
      "https://example.com/details"
    );
    expect(screen.getByRole("link", { name: "Apply now" })).toHaveAttribute("href", "https://example.com/apply");

    fireEvent.keyDown(window, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("button", { name: "Close" })).not.toBeInTheDocument());

    fireEvent.click(screen.getByText("Software Engineer", { selector: "h3" }));
    expect(await screen.findByRole("button", { name: "Close" })).toBeInTheDocument();
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(4));

    fireEvent.click(screen.getByRole("button", { name: "Close" }));
    await waitFor(() => expect(screen.queryByRole("button", { name: "Close" })).not.toBeInTheDocument());
  });

  it("returns early when a position without an id is opened", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [{ name: "No Id", company: "amazon", locations: [] }],
            total_results: 1,
            pagination_index: 1,
            has_next_page: false
          }
        })
      )
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [
              {
                id: "job-1",
                name: "Role One",
                company: "amazon",
                locations: [{ city: "Seattle", region: "Washington", country: "United States" }]
              }
            ],
            total_results: 1,
            page_size: 25,
            pagination_index: 1,
            has_next_page: false
          }
        })
      );

    render(<App />);
    await openResultsPopup();

    const label = await screen.findByText("No Id");
    const row = label.closest("button");
    const reactPropsKey = Object.keys(row).find((key) => key.startsWith("__reactProps"));
    expect(reactPropsKey).toBeTruthy();

    row[reactPropsKey].onClick();

    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(3));
    expect(screen.queryByRole("button", { name: "Close" })).not.toBeInTheDocument();
  });

  it("submits posted-within filter with job search requests", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: { jobs: [], total_results: 0, pagination_index: 1, has_next_page: false }
        })
      )
      .mockImplementationOnce(() =>
        makeResponse({
          payload: { jobs: [], total_results: 0, pagination_index: 1, has_next_page: false }
        })
      );

    render(<App />);
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));

    fireEvent.change(screen.getByLabelText("Company"), { target: { value: "amazon" } });
    fireEvent.change(screen.getByLabelText("Posted"), { target: { value: "30d" } });
    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "python" } });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));

    await waitFor(() => expect(getCallsMatching("/get_jobs").length).toBe(1));
    expect(getLastPostedBody("/get_jobs")).toEqual({
      company: "amazon",
      query: "python",
      posted_within: "30d",
      job_type: null,
      country: null,
      region: null,
      city: null,
      pagination_index: 1
    });
  });

  it("submits job-type filter with job search requests", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: { jobs: [], total_results: 0, pagination_index: 1, has_next_page: false }
        })
      )
      .mockImplementationOnce(() =>
        makeResponse({
          payload: { jobs: [], total_results: 0, pagination_index: 1, has_next_page: false }
        })
      );

    render(<App />);
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));

    fireEvent.change(screen.getByLabelText("Company"), { target: { value: "amazon" } });
    fireEvent.change(screen.getByLabelText("Job Type"), { target: { value: "machine_learning_engineer" } });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));

    await waitFor(() => expect(getCallsMatching("/get_jobs").length).toBe(1));
    expect(getLastPostedBody("/get_jobs")).toEqual({
      company: "amazon",
      query: null,
      posted_within: null,
      job_type: "machine_learning_engineer",
      country: null,
      region: null,
      city: null,
      pagination_index: 1
    });
  });

  it("opens results popup after a submitted search without scrolling the page", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: { jobs: [], total_results: 0, pagination_index: 1, has_next_page: false }
        })
      )
      .mockImplementationOnce(() =>
        makeResponse({
          payload: { jobs: [], total_results: 0, pagination_index: 1, has_next_page: false }
        })
      );

    render(<App />);
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));

    fireEvent.click(screen.getByRole("button", { name: "Search" }));

    await waitFor(() => expect(getCallsMatching("/get_jobs").length).toBe(1));
    expect(await screen.findByRole("dialog", { name: "Search results" })).toBeInTheDocument();
  });

  it("does not auto-search when changing company after initial load", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon", "google"] } }))
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: { jobs: [], total_results: 0, pagination_index: 1, has_next_page: false }
        })
      )
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: { jobs: [], total_results: 0, pagination_index: 1, has_next_page: false }
        })
      );

    render(<App />);
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));

    fireEvent.change(screen.getByLabelText("Company"), { target: { value: "google" } });
    await waitFor(() => expect(screen.getByLabelText("Company")).toHaveValue("google"));
    expect(getCallsMatching("/get_jobs").length).toBe(0);
    expect(screen.queryByRole("dialog", { name: "Search results" })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    await waitFor(() => expect(getCallsMatching("/get_jobs").length).toBe(1));
    expect(getLastPostedBody("/get_jobs")).toEqual({
      company: "google",
      query: null,
      posted_within: null,
      job_type: null,
      country: null,
      region: null,
      city: null,
      pagination_index: 1
    });
  });

  it("shows company-load fallback errors and empty companies state", async () => {
    fetch.mockImplementationOnce(() => makeResponse({ payload: {} }));

    render(<App />);

    expect(await screen.findByText("No companies returned by API.")).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "No companies" })).toBeInTheDocument();
  });

  it("shows default company-load error message for non-Error failures", async () => {
    fetch.mockImplementationOnce(() => Promise.reject("network-down"));
    render(<App />);
    expect(await screen.findByText("Failed to load companies.")).toBeInTheDocument();
  });

  it("handles details error response and payload-without-job", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
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
    await openResultsPopup();

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
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
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
    await openResultsPopup();
    expect(await screen.findByText("Role X")).toBeInTheDocument();
    fireEvent.click(screen.getByText("Role X"));
    expect(await screen.findByText("Failed to load details.")).toBeInTheDocument();
  });

  it("shows open-posting fallback action when apply link is missing", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
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
    await openResultsPopup();

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
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
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
    await openResultsPopup();

    expect(await screen.findByText("Role Abort")).toBeInTheDocument();
    fireEvent.click(screen.getByText("Role Abort"));
    expect(await screen.findByText("Loading details...")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Close" }));
    await waitFor(() => expect(screen.queryByRole("button", { name: "Close" })).not.toBeInTheDocument());
  });

  it("keeps modal open on non-escape keydown and handles non-Error details failure", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
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
    await openResultsPopup();
    expect(await screen.findByText("Positions")).toBeInTheDocument();
    await waitFor(() => expect(document.querySelector("button.result-item.row-clickable")).not.toBeNull());
    const row = document.querySelector("button.result-item.row-clickable");
    fireEvent.click(row);
    expect(await screen.findByText("Failed to load details.")).toBeInTheDocument();

    fireEvent.keyDown(window, { key: "Enter" });
    expect(screen.getByRole("button", { name: "Close" })).toBeInTheDocument();
  });

  it("supports next and previous pagination and company switching", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon", "google"] } }))
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
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
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
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
    await openResultsPopup();

    expect(await screen.findByText("A1")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    expect(await screen.findByText("A2")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Previous" }));
    expect(await screen.findByText("A1")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Company"), { target: { value: "google" } });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(await screen.findByText("G1")).toBeInTheDocument();
  });

  it("handles pagination-index fallback and search error reset", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
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
    await openResultsPopup();

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
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
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
    await openResultsPopup();
    expect(await screen.findByText("No jobs found on this page.")).toBeInTheDocument();
    expect(screen.getByText("Page 1")).toBeInTheDocument();
  });

  it("handles non-Error search failure with default message", async () => {
    fetch.mockImplementation((url) => {
      if (String(url).includes("/get_companies")) {
        return makeResponse({ payload: { companies: ["amazon"] } });
      }
      if (String(url).includes("/get_location_filters")) {
        return makeResponse(LOCATION_FILTERS_PAYLOAD);
      }
      if (String(url).includes("/get_jobs")) {
        return Promise.reject("jobs-down");
      }
      throw new Error(`Unexpected url: ${String(url)}`);
    });

    render(<App />);
    await waitFor(() => expect(getCallsMatching("/get_location_filters").length).toBeGreaterThanOrEqual(1));
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(await screen.findByText("Search request failed.")).toBeInTheDocument();
    expect(screen.queryByRole("dialog", { name: "Search results" })).not.toBeInTheDocument();
  });

  it("submits search queries and supports all-companies selection", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon", "google"] } }))
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [{ id: "role-1", name: "Role One", company: "amazon", locations: [] }],
            total_results: 1,
            page_size: 25,
            pagination_index: 1,
            has_next_page: false
          }
        })
      )
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [{ id: "role-3", name: "Role Three", company: "google", locations: [] }],
            total_results: 1,
            page_size: 25,
            pagination_index: 1,
            has_next_page: false
          }
        })
      );

    render(<App />);
    await openResultsPopup();

    expect(await screen.findByText("Role One")).toBeInTheDocument();
    expect(getCallsMatching("/get_jobs").length).toBe(1);
    expect(getLastPostedBody("/get_jobs")).toEqual({
      company: null,
      query: null,
      posted_within: null,
      job_type: null,
      country: null,
      region: null,
      city: null,
      pagination_index: 1
    });

    const searchInput = screen.getByRole("searchbox");
    fireEvent.change(searchInput, { target: { value: "  python  " } });
    fireEvent.change(screen.getByLabelText("Posted"), { target: { value: "7d" } });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(await screen.findByText("Role Three")).toBeInTheDocument();

    expect(getLastPostedBody("/get_jobs")).toEqual({
      company: null,
      query: "python",
      posted_within: "7d",
      job_type: null,
      country: null,
      region: null,
      city: null,
      pagination_index: 1
    });
  });

  it("loads location dropdown options and submits country region city filters", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            countries: ["United States"],
            regions: ["Washington"],
            cities: ["Seattle", "Tacoma"]
          }
        })
      )
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            countries: ["United States"],
            regions: ["Washington"],
            cities: ["Seattle"]
          }
        })
      )
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            countries: ["United States"],
            regions: ["Washington"],
            cities: ["Seattle"]
          }
        })
      )
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [],
            total_results: 0,
            page_size: 25,
            pagination_index: 1,
            has_next_page: false
          }
        })
      )
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            jobs: [],
            total_results: 0,
            page_size: 25,
            pagination_index: 1,
            has_next_page: false
          }
        })
      );

    render(<App />);
    await waitFor(() => expect(getCallsMatching("/get_location_filters").length).toBe(1));
    fireEvent.click(screen.getByRole("button", { name: "Location" }));
    expect(screen.getByRole("option", { name: "United States" })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: "Washington" })).not.toBeInTheDocument();
    expect(screen.queryByRole("option", { name: "Seattle" })).not.toBeInTheDocument();

    const countrySelect = screen.getByLabelText("Country");
    const countryProps = getReactProps(countrySelect);

    expect(countryProps?.onChange).toBeTypeOf("function");

    await act(async () => {
      countryProps.onChange({ target: { value: "United States" } });
    });
    await waitFor(() => expect(screen.getByLabelText("Region")).toBeInTheDocument());
    expect(screen.getByRole("option", { name: "Washington" })).toBeInTheDocument();

    const regionSelect = screen.getByLabelText("Region");
    const regionProps = getReactProps(regionSelect);
    expect(regionProps?.onChange).toBeTypeOf("function");
    await act(async () => {
      regionProps.onChange({ target: { value: "Washington" } });
    });
    await waitFor(() => expect(getCallsMatching("/get_location_filters").length).toBeGreaterThanOrEqual(2));

    expect(screen.getByRole("option", { name: "Seattle" })).toBeInTheDocument();
    const citySelect = screen.getByLabelText("City");
    const cityProps = getReactProps(citySelect);
    expect(cityProps?.onChange).toBeTypeOf("function");
    await act(async () => {
      cityProps.onChange({ target: { value: "Seattle" } });
    });
    expect(screen.getByText("Seattle, Washington, United States")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Clear location" }));
    expect(screen.getByText("Any location")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Search" }));

    await waitFor(() => expect(getCallsMatching("/get_jobs").length).toBe(1));
    expect(getLastPostedBody("/get_jobs")).toEqual({
      company: null,
      query: null,
      posted_within: null,
      job_type: null,
      country: null,
      region: null,
      city: null,
      pagination_index: 1
    });
    expect(getCallsMatching("/get_location_filters").some(([url]) => String(url).includes("region=Washington"))).toBe(
      true
    );
  });

  it("closes location panel and results popup via escape, outside click, backdrop, and button", async () => {
    fetch.mockImplementation((url) => {
      if (String(url).includes("/get_companies")) {
        return makeResponse({ payload: { companies: ["amazon"] } });
      }
      if (String(url).includes("/get_location_filters")) {
        return makeResponse(LOCATION_FILTERS_PAYLOAD);
      }
      if (String(url).includes("/get_jobs")) {
        return makeResponse({
          payload: {
            jobs: [
              {
                id: "job-1",
                name: "Role One",
                company: "amazon",
                locations: [{ city: "Seattle", region: "Washington", country: "United States" }]
              }
            ],
            total_results: 1,
            page_size: 25,
            pagination_index: 1,
            has_next_page: false
          }
        });
      }
      throw new Error(`Unexpected url: ${String(url)}`);
    });

    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Location" }));
    expect(screen.getByLabelText("Country")).toBeInTheDocument();
    fireEvent.keyDown(window, { key: "Escape" });
    await waitFor(() => expect(screen.queryByLabelText("Country")).not.toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: "Location" }));
    expect(screen.getByLabelText("Country")).toBeInTheDocument();
    fireEvent.mouseDown(window.document.body);
    await waitFor(() => expect(screen.queryByLabelText("Country")).not.toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    const dialog = await screen.findByRole("dialog", { name: "Search results" });
    expect(dialog).toBeInTheDocument();

    fireEvent.click(screen.getByText("Close results"));
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Search results" })).not.toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    await screen.findByRole("dialog", { name: "Search results" });
    fireEvent.click(screen.getByRole("presentation"));
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Search results" })).not.toBeInTheDocument());
  });

  it("keeps location panel open on internal interaction and shows selected company in results header", async () => {
    fetch.mockImplementation((url, options) => {
      if (String(url).includes("/get_companies")) {
        return makeResponse({ payload: { companies: ["amazon"] } });
      }
      if (String(url).includes("/get_location_filters")) {
        return makeResponse(LOCATION_FILTERS_PAYLOAD);
      }
      if (String(url).includes("/get_jobs")) {
        const body = JSON.parse(String(options?.body ?? "{}"));
        return makeResponse({
          payload: {
            jobs: [
              {
                id: "job-1",
                name: "Role One",
                company: body.company ?? "amazon",
                locations: [{ city: "Seattle", region: "Washington", country: "United States" }]
              }
            ],
            total_results: 1,
            page_size: 25,
            pagination_index: 1,
            has_next_page: false
          }
        });
      }
      throw new Error(`Unexpected url: ${String(url)}`);
    });

    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Location" }));
    expect(screen.getByLabelText("Country")).toBeInTheDocument();
    fireEvent.mouseDown(screen.getByLabelText("Country"));
    expect(screen.getByLabelText("Country")).toBeInTheDocument();
    fireEvent.keyDown(window, { key: "Enter" });
    expect(screen.getByLabelText("Country")).toBeInTheDocument();

    const companySelect = screen.getByLabelText("Company");
    const companyProps = getReactProps(companySelect);
    expect(companyProps?.onChange).toBeTypeOf("function");
    await act(async () => {
      companyProps.onChange({ target: { value: "amazon" } });
    });
    await waitFor(() => expect(screen.getByLabelText("Company")).toHaveValue("amazon"));
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(await screen.findByRole("dialog", { name: "Search results" })).toBeInTheDocument();
    expect(screen.getByText(/Amazon • 1 total jobs • Page 1/)).toBeInTheDocument();
  });

  it("shows all-companies prefix in results header when all companies is selected", async () => {
    fetch.mockImplementation((url) => {
      if (String(url).includes("/get_companies")) {
        return makeResponse({ payload: { companies: ["amazon"] } });
      }
      if (String(url).includes("/get_location_filters")) {
        return makeResponse(LOCATION_FILTERS_PAYLOAD);
      }
      if (String(url).includes("/get_jobs")) {
        return makeResponse({
          payload: {
            jobs: [
              {
                id: "job-1",
                name: "Role One",
                company: "amazon",
                locations: [{ city: "Seattle", region: "Washington", country: "United States" }]
              }
            ],
            total_results: 1,
            page_size: 25,
            pagination_index: 1,
            has_next_page: false
          }
        });
      }
      throw new Error(`Unexpected url: ${String(url)}`);
    });

    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(await screen.findByRole("dialog", { name: "Search results" })).toBeInTheDocument();
    expect(screen.getByText(/All Companies • 1 total jobs • Page 1/)).toBeInTheDocument();
  });

  it("omits company prefix in results header when no active company label exists", async () => {
    fetch.mockImplementation((url) => {
      if (String(url).includes("/get_companies")) {
        return Promise.reject(new Error("companies unavailable"));
      }
      if (String(url).includes("/get_jobs")) {
        return makeResponse({
          payload: {
            jobs: [
              {
                id: "job-1",
                name: "Role One",
                company: "amazon",
                locations: [{ city: "Seattle", region: "Washington", country: "United States" }]
              }
            ],
            total_results: 1,
            page_size: 25,
            pagination_index: 1,
            has_next_page: false
          }
        });
      }
      throw new Error(`Unexpected url: ${String(url)}`);
    });

    render(<App />);
    expect(await screen.findByText("companies unavailable")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(await screen.findByRole("dialog", { name: "Search results" })).toBeInTheDocument();
    const headerText = document.querySelector(".results-title p")?.textContent ?? "";
    expect(headerText).toContain("1 total jobs");
    expect(headerText).not.toContain("All Companies •");
    expect(headerText).not.toContain("Amazon •");
  });

  it("handles malformed location filter payloads without crashing", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() =>
        makeResponse({
          payload: {
            countries: "bad",
            regions: null,
            cities: 7
          }
        })
      );

    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Location" }));
    expect(screen.queryByRole("option", { name: "United States" })).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Region")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("City")).not.toBeInTheDocument();
  });

  it("shows location filter fetch errors and clears location selections", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() => Promise.reject(new Error("location filters unavailable")));

    render(<App />);

    expect(await screen.findByText("location filters unavailable")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Location" }));
    expect(screen.getByLabelText("Country")).toHaveValue("");
    expect(screen.queryByLabelText("Region")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("City")).not.toBeInTheDocument();
  });

  it("shows default location filter error message for non-Error failures", async () => {
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() => Promise.reject("location-filter-down"));

    render(<App />);

    expect(await screen.findByText("Failed to load location filters.")).toBeInTheDocument();
  });

  it("aborts previous details request when opening another row and falls back to Company label", async () => {
    let firstDetailsSignal;
    fetch
      .mockImplementationOnce(() => makeResponse({ payload: { companies: ["amazon"] } }))
      .mockImplementationOnce(() => makeResponse(LOCATION_FILTERS_PAYLOAD))
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
    await openResultsPopup();

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
