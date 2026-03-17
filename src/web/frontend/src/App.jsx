import { useEffect, useMemo, useRef, useState } from "react";
import { ArrowUpDown, Search } from "lucide-react";

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

function buildCompanyOptions(values) {
  const options = Array.isArray(values) ? values.map(toCompanyOption).filter(Boolean) : [];
  if (options.length === 0) {
    return [];
  }
  return [{ value: "__all__", label: "All Companies" }, ...options];
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

export function normalizePostedWithin(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  return normalized === "24h" || normalized === "7d" || normalized === "30d" ? normalized : "";
}

export function normalizeJobType(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  return normalized === "software_engineer" ||
    normalized === "machine_learning_engineer" ||
    normalized === "data_scientist" ||
    normalized === "manager"
    ? normalized
    : "";
}

export function normalizeJobLevel(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  return normalized === "intern" ||
    normalized === "junior" ||
    normalized === "mid" ||
    normalized === "senior" ||
    normalized === "staff" ||
    normalized === "principal" ||
    normalized === "distinguished" ||
    normalized === "fellow" ||
    normalized === "director" ||
    normalized === "unknown"
    ? normalized
    : "";
}

export function normalizeSearchMode(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  return normalized === "recency" ? "recency" : "relevance";
}

export function normalizeLocationFilter(value) {
  return String(value ?? "").trim();
}

export function retainSelectedOption(selectedValue, options) {
  return selectedValue && options.includes(selectedValue) ? selectedValue : "";
}

export function formatLocationFilterSummary(country, region, city) {
  const parts = [city, region, country].filter((value) => typeof value === "string" && value.trim());
  return parts.length > 0 ? parts.join(", ") : "Any location";
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
        const parts = [item.city, item.region || item.state, item.country]
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
  const resultsSectionRef = useRef(null);
  const locationFilterRef = useRef(null);
  const sortFilterRef = useRef(null);
  const [companyOptions, setCompanyOptions] = useState([]);
  const [selectedCompany, setSelectedCompany] = useState("");
  const [appliedCompany, setAppliedCompany] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [appliedQuery, setAppliedQuery] = useState("");
  const [postedWithin, setPostedWithin] = useState("");
  const [appliedPostedWithin, setAppliedPostedWithin] = useState("");
  const [searchMode, setSearchMode] = useState("relevance");
  const [appliedSearchMode, setAppliedSearchMode] = useState("relevance");
  const [jobType, setJobType] = useState("");
  const [appliedJobType, setAppliedJobType] = useState("");
  const [jobLevel, setJobLevel] = useState("");
  const [appliedJobLevel, setAppliedJobLevel] = useState("");
  const [country, setCountry] = useState("");
  const [region, setRegion] = useState("");
  const [city, setCity] = useState("");
  const [appliedCountry, setAppliedCountry] = useState("");
  const [appliedRegion, setAppliedRegion] = useState("");
  const [appliedCity, setAppliedCity] = useState("");
  const [locationOptions, setLocationOptions] = useState({ countries: [], regions: [], cities: [] });
  const [isLocationFilterOpen, setIsLocationFilterOpen] = useState(false);
  const [isSortFilterOpen, setIsSortFilterOpen] = useState(false);
  const [companiesLoading, setCompaniesLoading] = useState(false);

  const [positions, setPositions] = useState([]);
  const [totalResults, setTotalResults] = useState(null);
  const [pageSize, setPageSize] = useState(null);
  const [totalPagesFromApi, setTotalPagesFromApi] = useState(null);
  const [page, setPage] = useState(1);
  const [hasNextPage, setHasNextPage] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");
  const [isResultsOpen, setIsResultsOpen] = useState(false);

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
      const options = buildCompanyOptions(payload?.companies);
      if (options.length === 0) {
        throw new Error("No companies returned by API.");
      }
      setCompanyOptions(options);
      setSelectedCompany((previous) => {
        const next = chooseSelectedCompany(previous, options);
        setAppliedCompany(next);
        return next;
      });
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : "Failed to load companies.");
      setCompanyOptions([]);
      setSelectedCompany("");
      setAppliedCompany("");
      resetSearchResults(true);
    } finally {
      setCompaniesLoading(false);
    }
  }

  async function loadLocationFilters(
    companyValue = selectedCompany,
    selectedJobLevel = jobLevel,
    selectedCountry = country,
    selectedRegion = region
  ) {
    const params = new URLSearchParams();
    const normalizedCompany = normalizeCompany(companyValue);
    const normalizedJobLevel = normalizeJobLevel(selectedJobLevel);
    const normalizedCountry = normalizeLocationFilter(selectedCountry);
    const normalizedRegion = normalizeLocationFilter(selectedRegion);
    const normalizedCity = normalizeLocationFilter(city);
    if (normalizedCompany && normalizedCompany !== "__all__") {
      params.set("company", normalizedCompany);
    }
    if (normalizedJobLevel) {
      params.set("job_level", normalizedJobLevel);
    }
    if (normalizedCountry) {
      params.set("country", normalizedCountry);
    }
    if (normalizedRegion) {
      params.set("region", normalizedRegion);
    }
    try {
      const payload = await getJson(
        `${API_BASE_URL}/get_location_filters${params.toString() ? `?${params.toString()}` : ""}`
      );
      const nextCountries = Array.isArray(payload?.countries)
        ? payload.countries.filter((value) => typeof value === "string" && value.trim())
        : [];
      const nextRegions = Array.isArray(payload?.regions)
        ? payload.regions.filter((value) => typeof value === "string" && value.trim())
        : [];
      const nextCities = Array.isArray(payload?.cities)
        ? payload.cities.filter((value) => typeof value === "string" && value.trim())
        : [];
      setLocationOptions({
        countries: nextCountries,
        regions: nextRegions,
        cities: nextCities
      });
      setCountry(retainSelectedOption(normalizedCountry, nextCountries));
      setRegion(retainSelectedOption(normalizedRegion, nextRegions));
      setCity(retainSelectedOption(normalizedCity, nextCities));
    } catch (err) {
      setLocationOptions({ countries: [], regions: [], cities: [] });
      setCountry("");
      setRegion("");
      setCity("");
      setErrorMessage(err instanceof Error ? err.message : "Failed to load location filters.");
    }
  }

  async function searchJobs(
    targetPage,
    company = appliedCompany,
    query = appliedQuery,
    targetSearchMode = appliedSearchMode,
    timeWindow = appliedPostedWithin,
    targetJobType = appliedJobType,
    targetJobLevel = appliedJobLevel,
    targetCountry = appliedCountry,
    targetRegion = appliedRegion,
    targetCity = appliedCity,
    options = {}
  ) {
    const { openResultsOnSuccess = false } = options;
    const normalizedCompany = normalizeCompany(company);
    const normalizedQuery = String(query).trim();
    const normalizedSearchMode = normalizeSearchMode(targetSearchMode);
    const normalizedPostedWithin = normalizePostedWithin(timeWindow);
    const normalizedJobType = normalizeJobType(targetJobType);
    const normalizedJobLevel = normalizeJobLevel(targetJobLevel);
    const normalizedCountry = normalizeLocationFilter(targetCountry);
    const normalizedRegion = normalizeLocationFilter(targetRegion);
    const normalizedCity = normalizeLocationFilter(targetCity);

    const normalizedSearchBody = {
      company: normalizedCompany === "__all__" ? null : normalizedCompany,
      query: normalizedQuery || null,
      posted_within: normalizedPostedWithin || null,
      job_type: normalizedJobType || null,
      country: normalizedCountry || null,
      region: normalizedRegion || null,
      city: normalizedCity || null,
      pagination_index: targetPage
    };
    if (normalizedJobLevel) {
      normalizedSearchBody.job_level = normalizedJobLevel;
    }
    if (normalizedSearchMode === "recency") {
      normalizedSearchBody.search_mode = normalizedSearchMode;
    }

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
      if (openResultsOnSuccess) {
        setIsResultsOpen(true);
      }
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
    const effectiveCacheKey = `${position.runId ?? "run"}:${cacheKey}`;
    const cachedDetails = detailsCacheRef.current.get(effectiveCacheKey);

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
          company: String(position.company ?? ""),
          runId: typeof position.runId === "string" ? position.runId : null
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
      detailsCacheRef.current.set(effectiveCacheKey, normalizedDetails);
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
    if (companyOptions.length === 0) {
      return;
    }
    void loadLocationFilters(selectedCompany, jobLevel, country, region);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [companyOptions.length, selectedCompany, jobLevel, country, region]);

  const activeCompanyLabel = companyLabelByValue[appliedCompany] || toTitleCase(appliedCompany);
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
  const detailsJobLevel = normalizeJobLevel(activePosition?.jobLevel);
  const detailsCompanyLabel =
    companyLabelByValue[activePosition?.company] ||
    toTitleCase(activePosition?.company) ||
    "Company";
  const canGoPrevious = !isLoading && page > 1;
  const canGoNext = !isLoading && hasNextPage;

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

  useEffect(() => {
    if (!isLocationFilterOpen && !isSortFilterOpen) {
      return undefined;
    }
    const onPointerDown = (event) => {
      if (locationFilterRef.current && !locationFilterRef.current.contains(event.target)) {
        setIsLocationFilterOpen(false);
      }
      if (sortFilterRef.current && !sortFilterRef.current.contains(event.target)) {
        setIsSortFilterOpen(false);
      }
    };
    const onKeyDown = (event) => {
      if (event.key === "Escape") {
        setIsLocationFilterOpen(false);
        setIsSortFilterOpen(false);
      }
    };
    window.addEventListener("mousedown", onPointerDown);
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("mousedown", onPointerDown);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [isLocationFilterOpen, isSortFilterOpen]);

  function submitSearch(event) {
    event.preventDefault();
    const nextQuery = String(searchQuery).trim();
    setAppliedCompany(selectedCompany);
    setAppliedQuery(nextQuery);
    setAppliedSearchMode(searchMode);
    setAppliedPostedWithin(postedWithin);
    setAppliedJobType(jobType);
    setAppliedJobLevel(jobLevel);
    setAppliedCountry(country);
    setAppliedRegion(region);
    setAppliedCity(city);
    setIsLocationFilterOpen(false);
    setIsResultsOpen(false);
    void searchJobs(1, selectedCompany, nextQuery, searchMode, postedWithin, jobType, jobLevel, country, region, city, {
      openResultsOnSuccess: true
    });
  }

  return (
    <div className="page-shell">
      <header className="hero">
        <div className="topbar">
          <div className="brand-lockup" aria-label="Tier Zero mark">
            <span className="brand-monogram">T0</span>
          </div>
        </div>

        <div className="hero-center">
          <h1>Tier Zero</h1>
          <p className="hero-intro">Access the most selective tech roles at the world’s top companies.</p>

          <form className="search-panel search-panel-centered" onSubmit={submitSearch}>
            <div className="search-shell">
              <label className="search-input-wrap" aria-label="Search">
                <input
                  type="search"
                  value={searchQuery}
                  onChange={(event) => setSearchQuery(event.target.value)}
                  placeholder="Role, skill, or keyword — e.g. Staff Engineer"
                  disabled={isLoading}
                />
              </label>
              <button className="btn btn-primary search-submit-btn" type="submit" disabled={isLoading}>
                  <Search size={16} />
                  <span>Search</span>
              </button>
            </div>
            <div className="filter-section">
              <div className="subfilters">
              <label className="field company-select-field">
                <select
                  aria-label="Company"
                  value={selectedCompany}
                  onChange={(event) => {
                    setSelectedCompany(event.target.value);
                    setCountry("");
                    setRegion("");
                    setCity("");
                  }}
                  disabled={companiesLoading || companyOptions.length === 0}
                >
                  {companiesLoading && companyOptions.length === 0 ? (
                    <option value="">Loading</option>
                  ) : null}
                  {!companiesLoading && companyOptions.length === 0 ? (
                    <option value="">No companies</option>
                  ) : null}
                  {companyOptions.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
              <label className="field company-select-field">
                <select
                  aria-label="Posted"
                  value={postedWithin}
                  onChange={(event) => setPostedWithin(event.target.value)}
                >
                  <option value="">Any time</option>
                  <option value="24h">Last 24 hours</option>
                  <option value="7d">Last 7 days</option>
                  <option value="30d">Last 30 days</option>
                </select>
              </label>
              <label className="field company-select-field">
                <select
                  aria-label="Job Type"
                  value={jobType}
                  onChange={(event) => setJobType(event.target.value)}
                >
                  <option value="">All job types</option>
                  <option value="software_engineer">Software Engineer</option>
                  <option value="machine_learning_engineer">Machine Learning Engineer</option>
                  <option value="data_scientist">Data Scientist</option>
                  <option value="manager">Manager</option>
                </select>
              </label>
              <label className="field company-select-field">
                <select
                  aria-label="Job Level"
                  value={jobLevel}
                  onChange={(event) => setJobLevel(event.target.value)}
                >
                  <option value="">All job levels</option>
                  <option value="intern">Intern</option>
                  <option value="junior">Junior</option>
                  <option value="mid">Mid</option>
                  <option value="senior">Senior</option>
                  <option value="staff">Staff</option>
                  <option value="principal">Principal</option>
                  <option value="distinguished">Distinguished</option>
                  <option value="fellow">Fellow</option>
                  <option value="director">Director</option>
                </select>
              </label>
              <div className="field company-select-field location-filter-field" ref={locationFilterRef}>
                <button
                  type="button"
                  className="location-filter-trigger"
                  aria-label="Location"
                  aria-expanded={isLocationFilterOpen}
                  aria-controls="location-filter-panel"
                  onClick={() => {
                    setIsSortFilterOpen(false);
                    setIsLocationFilterOpen((previous) => !previous);
                  }}
                >
                  <span>{formatLocationFilterSummary(country, region, city)}</span>
                </button>
                {isLocationFilterOpen ? (
                  <div className="location-filter-panel" id="location-filter-panel">
                    <label className="field location-panel-field">
                      <select
                        aria-label="Country"
                        value={country}
                        onChange={(event) => {
                          setCountry(event.target.value);
                          setRegion("");
                          setCity("");
                        }}
                      >
                        <option value="">All countries</option>
                        {locationOptions.countries.map((option) => (
                          <option key={option} value={option}>
                            {option}
                          </option>
                        ))}
                      </select>
                    </label>
                    {country ? (
                      <label className="field location-panel-field">
                        <select
                          aria-label="Region"
                          value={region}
                          onChange={(event) => {
                            setRegion(event.target.value);
                            setCity("");
                          }}
                        >
                          <option value="">All regions</option>
                          {locationOptions.regions.map((option) => (
                            <option key={option} value={option}>
                              {option}
                            </option>
                          ))}
                        </select>
                      </label>
                    ) : null}
                    {region ? (
                      <label className="field location-panel-field">
                        <select
                          aria-label="City"
                          value={city}
                          onChange={(event) => setCity(event.target.value)}
                        >
                          <option value="">All cities</option>
                          {locationOptions.cities.map((option) => (
                            <option key={option} value={option}>
                              {option}
                            </option>
                          ))}
                        </select>
                      </label>
                    ) : null}
                    <div className="location-panel-actions">
                      <button
                        type="button"
                        className="btn btn-ghost"
                        onClick={() => {
                          setCountry("");
                          setRegion("");
                          setCity("");
                        }}
                      >
                        Clear location
                      </button>
                    </div>
                  </div>
                ) : null}
              </div>
              </div>
              <div className="field sort-filter-field" ref={sortFilterRef}>
                <button
                  type="button"
                  className="sort-filter-trigger"
                  aria-label="Sort by"
                  aria-expanded={isSortFilterOpen}
                  aria-controls="sort-filter-panel"
                  onClick={() => {
                    setIsLocationFilterOpen(false);
                    setIsSortFilterOpen((previous) => !previous);
                  }}
                  disabled={isLoading}
                >
                  <ArrowUpDown size={16} />
                  <span>Sort by</span>
                </button>
                {isSortFilterOpen ? (
                  <div className="sort-filter-panel" id="sort-filter-panel">
                    <div className="search-mode-radios" role="radiogroup" aria-label="Sort by">
                      <label className="search-mode-radio">
                        <input
                          type="radio"
                          name="search-mode"
                          value="relevance"
                          checked={searchMode === "relevance"}
                          onChange={() => setSearchMode("relevance")}
                        />
                        <span>Relevance</span>
                      </label>
                      <label className="search-mode-radio">
                        <input
                          type="radio"
                          name="search-mode"
                          value="recency"
                          checked={searchMode === "recency"}
                          onChange={() => setSearchMode("recency")}
                        />
                        <span>Latest</span>
                      </label>
                    </div>
                  </div>
                ) : null}
              </div>
            </div>
          </form>
          {errorMessage && !isResultsOpen && <div className="alert-error hero-alert">{errorMessage}</div>}
        </div>
      </header>

      {isResultsOpen && (
        <div className="results-backdrop" onClick={() => setIsResultsOpen(false)} role="presentation">
          <section
            className="results-panel"
            id="positions"
            ref={resultsSectionRef}
            role="dialog"
            aria-modal="true"
            aria-label="Search results"
            onClick={(event) => event.stopPropagation()}
          >
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
              <button className="btn btn-ghost" type="button" onClick={() => setIsResultsOpen(false)}>
                Close results
              </button>
            </div>

            {errorMessage && <div className="alert-error">{errorMessage}</div>}

            <div className="results-list" role="list">
              {!isLoading &&
                positions.map((position, index) => {
                  const resultJobLevel = normalizeJobLevel(position.jobLevel);
                  const resultCompanyLabel =
                    companyLabelByValue[position.company] || toTitleCase(position.company) || "—";
                  const resultCompanyMeta = resultJobLevel
                    ? `${resultCompanyLabel}, ${toTitleCase(resultJobLevel)}`
                    : resultCompanyLabel;
                  return (
                    <button
                      key={`${position.company ?? "company"}-${position.id ?? position.detailsUrl ?? position.applyUrl ?? index}`}
                      type="button"
                      role="listitem"
                      style={{ "--row-index": index }}
                      onClick={() => openDetails(position)}
                      className={`result-item ${position.id ? "row-clickable" : "row-disabled"}`}
                      disabled={!position.id}
                    >
                      <div className="result-item-main">
                        <div className="result-item-topline">
                          <span className="result-company">{resultCompanyMeta}</span>
                          <span className="result-posted">{formatPosted(position.postedTs)}</span>
                        </div>
                        <h3 className="result-title">{position.name || "—"}</h3>
                        <p className="result-location" title={formatLocations(position.locations, 99)}>
                          {formatLocations(position.locations)}
                        </p>
                      </div>
                    </button>
                  );
                })}

              {!isLoading && positions.length === 0 && (
                <div className="empty-state" role="status">
                  No jobs found on this page.
                </div>
              )}

              {isLoading && (
                <div className="loading-state" role="status">
                  Loading positions...
                </div>
              )}
            </div>

            <div className="pagination">
              <button
                className="btn btn-ghost"
                type="button"
                disabled={!canGoPrevious}
                onClick={() =>
                  void searchJobs(
                    page - 1,
                    appliedCompany,
                    appliedQuery,
                    appliedSearchMode,
                    appliedPostedWithin,
                    appliedJobType,
                    appliedJobLevel,
                    appliedCountry,
                    appliedRegion,
                    appliedCity
                  )
                }
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
                disabled={!canGoNext}
                onClick={() =>
                  void searchJobs(
                    page + 1,
                    appliedCompany,
                    appliedQuery,
                    appliedSearchMode,
                    appliedPostedWithin,
                    appliedJobType,
                    appliedJobLevel,
                    appliedCountry,
                    appliedRegion,
                    appliedCity
                  )
                }
              >
                Next
              </button>
            </div>
          </section>
        </div>
      )}

      {isModalOpen && (
        <div className="modal-backdrop" onClick={closeModal} role="presentation">
          <div className="modal-card" onClick={(event) => event.stopPropagation()}>
            {(detailsLoading || detailsError || !jobDetails) && (
              <div className="modal-topbar">
                <button className="modal-close" onClick={closeModal} type="button">
                  Close
                </button>
              </div>
            )}

            {detailsLoading && <p className="loading-state">Loading details...</p>}
            {detailsError && <div className="alert-error">{detailsError}</div>}

            {!detailsLoading && !detailsError && jobDetails && (
              <div className="details-content">
                <div className="details-header">
                  <h3>{detailsName}</h3>
                  <button className="modal-close" onClick={closeModal} type="button">
                    Close
                  </button>
                </div>

                {(detailsPageHref || applyHref || detailsPostedTs) && (
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
                    {detailsPostedTs && <div className="details-posted">{formatPosted(detailsPostedTs)}</div>}
                  </div>
                )}

                <section className="details-section">
                  <h4>Company</h4>
                  <p className="details-value">{detailsCompanyLabel}</p>
                </section>

                {detailsJobLevel && (
                  <section className="details-section">
                    <h4>Level</h4>
                    <p className="details-value">{toTitleCase(detailsJobLevel)}</p>
                  </section>
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
