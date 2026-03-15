import { useEffect, useMemo, useRef, useState } from "react";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000";

export function toTitleCase(value) {
  if (!value) {
    return "";
  }
  return value
    .split(/[\s_-]+/)
    .filter(Boolean)
    .map((part) => part[0].toUpperCase() + part.slice(1))
    .join(" ");
}

export function toCompanyOption(value) {
  if (typeof value !== "string") {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    return null;
  }
  return {
    value: normalized,
    label: toTitleCase(normalized)
  };
}

export function chooseSelectedCompany(previous, options) {
  if (previous && options.some((option) => option.value === previous)) {
    return previous;
  }
  return options[0]?.value ?? "";
}

export function normalizeCompany(value) {
  return String(value ?? "").trim();
}

export function formatPosted(ts) {
  if (!ts) {
    return "—";
  }
  return new Date(ts * 1000).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric"
  });
}

export function formatLocations(locations, maxItems = 2) {
  if (!Array.isArray(locations) || locations.length === 0) {
    return "—";
  }
  const cleaned = locations
    .map((item) => {
      if (typeof item === "string") {
        return item.trim();
      }
      if (item && typeof item === "object") {
        const parts = [item.city, item.state, item.country]
          .map((value) => (typeof value === "string" ? value.trim() : ""))
          .filter(Boolean);
        return parts.join(", ");
      }
      return "";
    })
    .filter(Boolean);
  if (cleaned.length === 0) {
    return "—";
  }
  if (cleaned.length <= maxItems) {
    return cleaned.join(" • ");
  }
  return `${cleaned.slice(0, maxItems).join(" • ")} +${cleaned.length - maxItems} more`;
}

export function normalizeDescription(raw) {
  if (!raw) {
    return "";
  }
  return raw
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n\n")
    .replace(/<li>/gi, "• ")
    .replace(/<\/li>/gi, "\n")
    .replace(/<[^>]*>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

export async function extractError(response) {
  try {
    const payload = await response.json();
    if (typeof payload?.detail === "string") {
      return payload.detail;
    }
    if (payload?.detail) {
      return JSON.stringify(payload.detail);
    }
  } catch (err) {
    return `${response.status} ${response.statusText}`;
  }
  return `${response.status} ${response.statusText}`;
}

export async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  if (!response.ok) {
    throw new Error(await extractError(response));
  }
  return response.json();
}

export async function getJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(await extractError(response));
  }
  return response.json();
}

export default function App() {
  const [companyOptions, setCompanyOptions] = useState([]);
  const [selectedCompany, setSelectedCompany] = useState("");
  const [companiesLoading, setCompaniesLoading] = useState(false);

  const [positions, setPositions] = useState([]);
  const [totalResults, setTotalResults] = useState(null);
  const [pageSize, setPageSize] = useState(null);
  const [totalPagesFromApi, setTotalPagesFromApi] = useState(null);
  const [page, setPage] = useState(1);
  const [hasNextPage, setHasNextPage] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [detailsLoading, setDetailsLoading] = useState(false);
  const [detailsError, setDetailsError] = useState("");
  const [jobDetails, setJobDetails] = useState(null);
  const [activePosition, setActivePosition] = useState(null);
  const detailsCacheRef = useRef(new Map());
  const detailsAbortRef = useRef(null);

  const companyLabelByValue = useMemo(
    () => Object.fromEntries(companyOptions.map((option) => [option.value, option.label])),
    [companyOptions]
  );
  const detailsPageHref = useMemo(
    () => (jobDetails && typeof jobDetails === "object" ? jobDetails.detailsUrl : "") || activePosition?.detailsUrl || "",
    [activePosition, jobDetails]
  );
  const applyHref = useMemo(
    () => jobDetails?.applyUrl || activePosition?.applyUrl || "",
    [activePosition, jobDetails]
  );
  const openPostingHref = useMemo(
    () => {
      return (
        detailsPageHref ||
        applyHref ||
        ""
      );
    },
    [applyHref, detailsPageHref]
  );

  function resetSearchResults(resetPage = false) {
    setPositions([]);
    setTotalResults(null);
    setPageSize(null);
    setTotalPagesFromApi(null);
    setHasNextPage(false);
    if (resetPage) {
      setPage(1);
    }
  }

  async function loadCompanies() {
    setCompaniesLoading(true);
    try {
      const payload = await getJson(`${API_BASE_URL}/get_companies`);
      const options = Array.isArray(payload?.companies)
        ? payload.companies.map(toCompanyOption).filter(Boolean)
        : [];
      if (options.length === 0) {
        throw new Error("No companies returned by API.");
      }
      setCompanyOptions(options);
      setSelectedCompany((previous) => chooseSelectedCompany(previous, options));
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : "Failed to load companies.");
      setCompanyOptions([]);
      setSelectedCompany("");
      resetSearchResults(true);
    } finally {
      setCompaniesLoading(false);
    }
  }

  async function searchJobs(targetPage, company = selectedCompany) {
    const normalizedCompany = normalizeCompany(company);

    const normalizedSearchBody = {
      company: normalizedCompany,
      pagination_index: targetPage
    };

    setIsLoading(true);
    setErrorMessage("");

    try {
      const payload = await postJson(`${API_BASE_URL}/get_jobs`, normalizedSearchBody);
      const merged = Array.isArray(payload?.jobs) ? payload.jobs : [];
      setPositions(merged);
      setTotalResults(typeof payload?.total_results === "number" ? payload.total_results : null);
      setPageSize(typeof payload?.page_size === "number" && payload.page_size > 0 ? payload.page_size : null);
      setTotalPagesFromApi(
        typeof payload?.total_pages === "number" && payload.total_pages > 0 ? payload.total_pages : null
      );
      setHasNextPage(Boolean(payload?.has_next_page));
      setPage(
        typeof payload?.pagination_index === "number"
          ? payload.pagination_index
          : targetPage
      );
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : "Search request failed.");
      resetSearchResults();
    } finally {
      setIsLoading(false);
    }
  }

  async function openDetails(position) {
    if (!position?.id) {
      return;
    }

    const cacheKey = `${position.company ?? "company"}:${position.id}`;
    const cachedDetails = detailsCacheRef.current.get(cacheKey);

    setActivePosition(position);
    setIsModalOpen(true);
    setDetailsError("");
    if (cachedDetails) {
      setJobDetails(cachedDetails);
      setDetailsLoading(false);
      return;
    }

    if (detailsAbortRef.current) {
      detailsAbortRef.current.abort();
    }
    const controller = new AbortController();
    detailsAbortRef.current = controller;

    setDetailsLoading(true);
    setJobDetails(null);

    try {
      const response = await fetch(`${API_BASE_URL}/get_job_details`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          job_id: String(position.id),
          company: String(position.company ?? "")
        }),
        signal: controller.signal
      });
      if (!response.ok) {
        throw new Error(await extractError(response));
      }

      const payload = await response.json();
      if (typeof payload?.jobDescription !== "string") {
        throw new Error(
          typeof payload?.error === "string" ? payload.error : "Failed to load details."
        );
      }

      const normalizedDetails = {
        jobDescription: payload.jobDescription,
        skills: Array.isArray(payload?.skills) ? payload.skills.filter((item) => typeof item === "string") : [],
        detailsUrl: typeof payload?.detailsUrl === "string" ? payload.detailsUrl : ""
      };
      detailsCacheRef.current.set(cacheKey, normalizedDetails);
      setJobDetails(normalizedDetails);
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") {
        return;
      }
      setDetailsError(err instanceof Error ? err.message : "Failed to load details.");
    } finally {
      if (detailsAbortRef.current === controller) {
        detailsAbortRef.current = null;
      }
      setDetailsLoading(false);
    }
  }

  function closeModal() {
    if (detailsAbortRef.current) {
      detailsAbortRef.current.abort();
      detailsAbortRef.current = null;
    }
    setIsModalOpen(false);
    setDetailsLoading(false);
    setDetailsError("");
    setJobDetails(null);
    setActivePosition(null);
  }

  useEffect(() => {
    void loadCompanies();
  }, []);

  useEffect(() => {
    if (!selectedCompany) {
      return;
    }
    void searchJobs(1, selectedCompany);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedCompany]);

  const activeCompanyLabel = companyLabelByValue[selectedCompany] || toTitleCase(selectedCompany);
  const totalPages =
    typeof totalPagesFromApi === "number"
      ? totalPagesFromApi
      : (typeof totalResults === "number" && typeof pageSize === "number" && pageSize > 0
      ? Math.max(1, Math.ceil(totalResults / pageSize))
      : null);
  const detailsName = activePosition?.name || "Job Details";
  const detailsPostedTs =
    typeof activePosition?.postedTs === "number" ? activePosition.postedTs : null;
  const detailsLocations = Array.isArray(activePosition?.locations) ? activePosition.locations : [];
  const detailsCompanyLabel =
    companyLabelByValue[activePosition?.company] ||
    toTitleCase(activePosition?.company) ||
    "Company";

  useEffect(() => {
    if (!isModalOpen) {
      return undefined;
    }
    const onKeyDown = (event) => {
      if (event.key === "Escape") {
        closeModal();
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [isModalOpen]);

  return (
    <div className="page-shell">
      <div className="orb orb-a" />
      <div className="orb orb-b" />

      <header className="hero">
        <p className="hero-kicker">Multi-Company Careers Explorer</p>
        <h1>All Jobs Feed</h1>
        <p>
          Browse a paginated feed of Amazon, Apple, Google, Meta, Microsoft, and Netflix jobs, then inspect details.
        </p>
      </header>

      <section className="results-card">
        <div className="results-header">
          <div className="results-title">
            <h2>Positions</h2>
            <p>
              {activeCompanyLabel ? `${activeCompanyLabel} • ` : ""}
              {typeof totalResults === "number" ? `${totalResults.toLocaleString("en-US")} total jobs • ` : ""}
              Page {page}
              {typeof totalPages === "number" ? ` of ${totalPages.toLocaleString("en-US")}` : ""}
            </p>
          </div>
          <label className="field company-select-field">
            <span>Company</span>
            <select
              value={selectedCompany}
              onChange={(event) => setSelectedCompany(event.target.value)}
              disabled={companiesLoading || isLoading || companyOptions.length === 0}
            >
              {companiesLoading && companyOptions.length === 0 && (
                <option value="">Loading companies...</option>
              )}
              {!companiesLoading && companyOptions.length === 0 && (
                <option value="">No companies</option>
              )}
              {companyOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
        </div>

        {errorMessage && <div className="alert-error">{errorMessage}</div>}

        <div className="table-wrap">
          <table className="positions-table">
            <thead>
              <tr>
                <th>Company</th>
                <th>Name</th>
                <th>Locations</th>
                <th>Posted</th>
              </tr>
            </thead>
            <tbody>
              {!isLoading &&
                positions.map((position, index) => (
                  <tr
                    key={`${position.company ?? "company"}-${position.id ?? position.detailsUrl ?? position.applyUrl ?? index}`}
                    style={{ "--row-index": index }}
                    onClick={() => openDetails(position)}
                    className={position.id ? "row-clickable" : "row-disabled"}
                  >
                    <td>{companyLabelByValue[position.company] || toTitleCase(position.company) || "—"}</td>
                    <td>{position.name || "—"}</td>
                    <td className="location-cell" title={formatLocations(position.locations, 99)}>
                      {formatLocations(position.locations)}
                    </td>
                    <td>{formatPosted(position.postedTs)}</td>
                  </tr>
                ))}

              {!isLoading && positions.length === 0 && (
                <tr>
                  <td colSpan={4} className="empty-state">
                    No jobs found on this page.
                  </td>
                </tr>
              )}

              {isLoading && (
                <tr>
                  <td colSpan={4} className="loading-state">
                    Loading positions...
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div className="pagination">
          <button
            className="btn btn-ghost"
            type="button"
            disabled={isLoading || page === 1 || !selectedCompany}
            onClick={() => void searchJobs(page - 1)}
          >
            Previous
          </button>
          <span>
            Page {page}
            {typeof totalPages === "number" ? ` of ${totalPages.toLocaleString("en-US")}` : ""}
          </span>
          <button
            className="btn btn-ghost"
            type="button"
            disabled={isLoading || !hasNextPage || !selectedCompany}
            onClick={() => void searchJobs(page + 1)}
          >
            Next
          </button>
        </div>
      </section>

      {isModalOpen && (
        <div className="modal-backdrop" onClick={closeModal} role="presentation">
          <div className="modal-card" onClick={(event) => event.stopPropagation()}>
            <button className="modal-close" onClick={closeModal} type="button">
              Close
            </button>

            {detailsLoading && <p className="loading-state">Loading details...</p>}
            {detailsError && <div className="alert-error">{detailsError}</div>}

            {!detailsLoading && !detailsError && jobDetails && (
              <div className="details-content">
                <h3>{detailsName}</h3>
                <div className="details-meta">
                  <span>{detailsCompanyLabel}</span>
                  <span>Posted: {formatPosted(detailsPostedTs)}</span>
                </div>

                {(detailsPageHref || applyHref) && (
                  <div className="details-actions">
                    {detailsPageHref && (
                      <a
                        className="btn btn-ghost details-action-btn"
                        href={detailsPageHref}
                        target="_blank"
                        rel="noreferrer"
                      >
                        View details page
                      </a>
                    )}
                    {applyHref && (
                      <a
                        className="btn btn-primary details-action-btn"
                        href={applyHref}
                        target="_blank"
                        rel="noreferrer"
                      >
                        Apply now
                      </a>
                    )}
                    {!applyHref && openPostingHref && (
                      <a
                        className="btn btn-primary details-action-btn"
                        href={openPostingHref}
                        target="_blank"
                        rel="noreferrer"
                      >
                        Open posting
                      </a>
                    )}
                  </div>
                )}

                {detailsLocations.length > 0 && (
                  <section className="details-section">
                    <h4>Locations</h4>
                    <ul className="details-list">
                      {detailsLocations.map((place, index) => (
                        <li key={`${formatLocations([place], 1)}-${index}`}>
                          {formatLocations([place], 1)}
                        </li>
                      ))}
                    </ul>
                  </section>
                )}

                {Array.isArray(jobDetails?.skills) && jobDetails.skills.length > 0 && (
                  <section className="details-section">
                    <h4>Skills</h4>
                    <div className="skills-chip-list">
                      {jobDetails.skills.map((skill) => (
                        <span key={skill} className="skill-chip">
                          {skill}
                        </span>
                      ))}
                    </div>
                  </section>
                )}

                {jobDetails && (
                  <section className="details-section">
                    <h4>Job Description</h4>
                    <article className="description-block">
                      <p>{normalizeDescription(jobDetails.jobDescription)}</p>
                    </article>
                  </section>
                )}

              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
