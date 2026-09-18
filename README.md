# CDC WONDER scraper

Produces every file under `raw/`. CDC WONDER has no API for sub-national
data (its official API is national-only by policy), so this drives the live
query forms with Selenium, one query per year x geography-level x
demographic breakdown, and downloads each result CSV.

Two scripts:

- `cdc_wonder_puller.py` -- the scraper. Parameterized by database, cause of
  death, geography (national / state / county), age grouping and demographic
  breakdowns; everything database-specific is in `form_config.json`. Run it
  directly for any new pull.
- `drowning_scraper.py` -- the drowning parameters (ICD-10 W65-W74 on the
  Underlying Cause of Death 2018-2024 database, national + state + county,
  Ten-Year ages, plus the Five-Year county population query `ingest.R`
  needs). It just hands its command line to the puller; every puller flag
  works on it too.

## Running it

```bash
# one-time: a venv for the scraper's deps. Do NOT pip install these into the
# anaconda base env -- doing so upgraded numpy/pandas there and broke
# matplotlib/scipy until they were restored by hand.
/opt/anaconda3/bin/python3 -m venv scraper/.venv
scraper/.venv/bin/python -m pip install selenium webdriver-manager \
    "git+https://github.com/govex/cdc-wonder-scraper.git"

# the drowning pull (fills whatever is missing under raw/ucd_icd10_expanded_2018_2024/)
caffeinate -i -s scraper/.venv/bin/python scraper/drowning_scraper.py

# any other pull, e.g. drowning as a contributing cause, national + state only
caffeinate -i -s scraper/.venv/bin/python scraper/cdc_wonder_puller.py \
    --dataset mcd_icd10_expanded_2018_2024 --codes W65-W74 --tree-path V01-Y89 \
    --cause-axis multiple --levels national state
```

Needs Chrome installed locally (`webdriver-manager` fetches a matching
chromedriver automatically). Runs headless by default (`--no-headless` to
watch) and a full county pull takes several hours -- state-level queries are
quick, but county-level is 51 states x 7 years per breakdown, each with a
built-in 20-40s randomized delay between requests (a uniform 6s delay got the
scraper blocked after ~14 hours; CDC documents no rate limit, so this is
empirical).

Output goes straight into `raw/<dataset key>/<year>/` in this repo -- the
tree `ingest.R` reads -- so there is nothing to copy afterwards; re-run
`ingest.R` (or `dcf::dcf_process()`) to regenerate `standard/`. Note the
default output directory is keyed by *database*, not by cause: a second cause
of death from the same database needs its own `--output-dir`.

Already-downloaded files are skipped on a re-run (checked by path, not
content), so it's safe to stop and resume, and a re-run only fills gaps.
Each download's footer is validated before it is kept (see `--verify`
below); a download that fails validation is discarded and counted as a
failure, and every run ends with a `N downloaded, M skipped, K FAILED` tally.

**Keep the machine awake for the duration** -- run it under `caffeinate -i -s`
on macOS. Without that, if the Mac sleeps mid-run, individual steps that
normally take seconds can stall for 10-40+ minutes once it wakes, alongside
scattered network-disconnect errors; a run can end up taking days instead of
hours. `caffeinate -i` alone is not enough if the lid gets closed -- that
force-sleeps regardless, so keep the lid open too.

### Checking what is there: `--check` and `--verify`

```bash
scraper/.venv/bin/python scraper/drowning_scraper.py --check            # expected vs present
scraper/.venv/bin/python scraper/drowning_scraper.py --check --verify   # ...and validate footers
```

`--check` lists every file the current parameters would produce and reports
which are missing (and any csv under the year folders that no current query
produces). `--verify` additionally parses the `Query Parameters:` footer every
WONDER export ends with and flags any file whose year, state, group-by
dimensions, age grouping, show-options or ICD codes disagree with its name.
Exit status is 1 if anything is missing or invalid, so it can gate a
pipeline. It needs no browser and runs in under a second on the ~1,800
drowning files.

This exists because two files (`2023/county_race_Nevada.csv` and
`2024/county_pop5yr_Nevada.csv`) were missing from the original drowning
pull for weeks without anyone noticing: the failure was one warning line in
a 700-line log, and `ingest.R` quietly absorbed the gap as
`missing_denom_flag = 1`. The same list now drives both the check and what a
run pulls, so a gap is reported offline and a re-run fills exactly it.

## Config: `form_config.json`

One entry per WONDER database, mapping the role names the puller uses
(`State`, `County`, `Year`, `Sex`, `Race`, `Hispanic Origin`, plus the
`age_groupings` and `cause_axes` blocks) to CDC WONDER's internal variable
codes (e.g. `D158.V5`). These codes aren't documented anywhere obvious; they
were found by loading each live form and reading each variable's actual
`<label>` text (see `wonder_form_fields.txt` for a raw dump of the D158
form's field IDs). Notably:

- Age grouping on the mortality databases is `Vxx.V5` = **Ten-Year Age
  Groups** (`<1`, `1-4`, `5-14`, `15-24`, ..., `85+`) -- deliberately not
  `V51` (Five-Year) or `V52` (Single-Year, easy to confuse with V5 as it's a
  different code entirely). `--verify` checks the exported footer says the
  right one.
- The mortality databases (D76, D77, D157, D158) share one variable
  numbering; the natality databases use a different one.
- Cause-of-death filters go through a "finder" widget whose every element id
  derives from one variable code (radio `RO_ucd<var>`/`RO_mcd<var>`, list
  `codes-<var>`, buttons `finder-action-<var>-Open Fully`, current-filter
  textarea `<var>-fhi`). `cause_axes` names the variable(s) per database.
- Adding a cause of death needs one manual lookup: which branch(es) of the
  ICD Browse tree to expand to reach it (`--tree-path`; `V01-Y89` for
  drowning). The finder's text-search box was tried as a way around this and
  did not work -- see the comment above `make_cause_selector` in the puller.

### Databases

| key | database | status (as of 2026-08-25) |
| --- | --- | --- |
| `ucd_icd10_expanded_2018_2024` | Underlying Cause of Death, 2018-2024, Single Race (D158) | in production -- the drowning pull, national + state + county |
| `ucd_icd10_1999_2020` | Underlying Cause of Death, 1999-2020, bridged race (D76) | smoke-tested (one state x age query): works. Older vintage still marks rates on <20 deaths `Unreliable`, a marker `ingest.R` does not know |
| `mcd_icd10_expanded_2018_2024` | Multiple Cause of Death, 2018-2024, Single Race (D157) | smoke-tested with `--cause-axis multiple` (a code anywhere on the certificate): works. Its finder is WONDER's "advanced" kind and needed its own handling -- see the entry's `_notes` |
| `mcd_icd10_1999_2020` | Multiple Cause of Death, 1999-2020, bridged race (D77) | codes verified on the live form; carried over from D76 + D157, not yet run |
| `natality_expanded_2016_2024` | Natality, 2016-2024 expanded (D149) | smoke-tested at state and county level: works. No `--codes`; `Age` is mother's age, `Sex` is infant's; export is a single `Births` column |
| `natality_2007_2024` | Natality, 2007-2024 (D66) | codes verified; carried over from D149, not yet run |

**Smoke-test any "not yet run" database before a full pull** -- one
state-level query with `--years Y Y --only state_gender` (add `--no-headless`
to watch) -- and record what you learn in that entry's `_notes`. Every
download is validated against its own footer, so a wrong guess about a form
shows up as a rejected file (kept under `$TMPDIR/cdc_wonder_rejected/` with
its parsed footer in the log) rather than as bad data in `raw/`. That is
how the two surprises so far were caught: natality words its footer
differently (`Year:` and `State of Residence`, now in the entry's `footer`
block), and D157's multiple-cause finder silently ignores a highlighted code
unless it is moved into its "Selected Items" box -- the first attempt came
back as an all-cause export, ~1,000 deaths per 100k instead of ~1.

The drowning `national_*.csv` files become the geography `"00"` rows of
`standard/data_state.csv.gz`. `ingest.R` reads them as pulled and never sums
states up to a national figure: state cells each hide 1-9 suppressed deaths,
so a state sum runs low (the 2024 national file says 4,201 where the state
cells sum to 4,087).

## Known CDC-side gaps (not scraper bugs)

- **County x Ten-Year age has no population.** Querying `County` x
  Ten-Year Age Groups returns Deaths only -- no Population or Crude Rate,
  for every state and year (confirmed against the live form: those measures
  stay checked/mandatory in the UI, the server just omits them; CDC's help
  page says the same for 10-year and single-year groups at county level).
  `ingest.R` works around it by deriving county population from the
  Five-Year county export (`county_pop5yr_*.csv`, the one extra query
  `drowning_scraper.py` adds) -- see the comments around
  `FIVE_TO_TEN_YEAR_AGE` there, including a further gap where population is
  simply unavailable for the `<1`, `1-4`, and `85+` buckets at the county
  level, for every county nationwide.
- **Connecticut, 2022 onward.** Connecticut replaced its 8 counties with 9
  planning regions in 2022. WONDER's 2018-2024 databases still code deaths
  to the 8 legacy counties (09001-09015) and report their Population as
  "Not Available" from 2022 on; they have no rows at all for the planning
  regions (09110-09190), and CDC's help states "Rates are not available for
  any grouping including specific counties in Connecticut in 2022 and
  later." So the raw pull is complete as WONDER offers it: Connecticut
  county death counts exist for 2022-2024, county rates do not, and nothing
  in the scraper can change that.
