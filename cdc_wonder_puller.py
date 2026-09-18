"""
CDC WONDER puller (generalized)
================================
The one scraper for this project's CDC WONDER pulls. It started life as a
parameterized version of drowning_scraper.py (drowning's W65-W74 on the
"Underlying Cause of Death, 2018-2024, Single Race" database) and now drives
any of the WONDER databases described in form_config.json -- underlying and
multiple cause of death in both the 1999-2020 and 2018-2024 vintages, and the
natality (births) databases -- at national, state, or county resolution.

Everything database-specific is in form_config.json (variable codes, year
range, which cause-of-death finders exist). This file only knows the *shape*
of a WONDER request form, which the govex cdc_wonder package drives: pick
group-by dimensions, a year, optionally a state, run a custom setup step (that
is where the cause-of-death filter goes), export as CSV. Nothing in the govex
package is database-specific either, which is what makes this generalization
cheap -- see the comment above make_cause_selector for the one place that
needed real work. ingest.R is deliberately NOT generalized; it stays
drowning-specific and reads the exact filenames this script writes.

Run (one-time install, then run). Use a venv -- do NOT pip install into the
anaconda base env; doing so on 2026-08-25 silently upgraded numpy/pandas there
and broke matplotlib/scipy until they were restored by hand:
    /opt/anaconda3/bin/python3 -m venv scraper/.venv
    scraper/.venv/bin/python -m pip install selenium webdriver-manager \\
        "git+https://github.com/govex/cdc-wonder-scraper.git"

Examples:
    # what the drowning pipeline pulls (drowning_scraper.py is exactly this)
    scraper/.venv/bin/python scraper/cdc_wonder_puller.py \\
        --dataset ucd_icd10_expanded_2018_2024 --codes W65-W74 --tree-path V01-Y89 \\
        --levels national state county --age ten_year \\
        --extra-query "county_pop5yr=County,Age[five_year]"

    # report which expected files are missing / malformed, download nothing
    scraper/.venv/bin/python scraper/cdc_wonder_puller.py ... --check --verify

    # from Python
    from cdc_wonder_puller import pull_cdc_wonder
    result = pull_cdc_wonder(
        dataset_key      = "mcd_icd10_expanded_2018_2024",
        codes            = "W65-W74",            # exact label text in WONDER's ICD tree
        tree_path        = ["V01-Y89"],          # chapter(s) to expand to reach it
        cause_axis       = "multiple",           # count W65-W74 anywhere on the certificate
        geography_levels = ["national", "state"],
    )

Output lands in raw/<dataset_key>/<year>/ inside this repo -- the tree
ingest.R reads -- as one CSV per query (county queries get one file per
state). Already-present files are skipped, so a run can be stopped and resumed
and a re-run only fills gaps. Note the default output dir is keyed by
*database*, not by cause: pulling a second cause of death from the same
database needs its own --output-dir or it would be mistaken for the first.

tree_path is a one-time manual lookup per new cause of death: load the
database's request form and note which branch(es) of the ICD Browse tree
you'd click through to reach your code -- the same kind of one-time discovery
this project already did for the variable codes in form_config.json. An
earlier version of this file tried to avoid that by using WONDER's text search
box instead of tree navigation, which would have generalized without needing
tree_path at all; that didn't pan out (see the comment above
make_cause_selector for what went wrong), so tree-clicking -- the mechanism
drowning_scraper.py proved reliable across 700+ live queries -- is what this
uses.

Keep the machine awake for the duration -- run under `caffeinate -i -s` on
macOS. A run that stalls mid-way because the machine slept can turn a few-hour
pull into multiple days; see scraper/README.md for what that looks like.
"""

import argparse
import logging
import random
import re
import shutil
import sys
import tempfile
import time
from collections import namedtuple
from pathlib import Path
from typing import Optional

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from cdc_wonder import wonder_query, load_form_config, US_STATES

SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parent
RAW_ROOT = REPO_ROOT / "raw"
DEFAULT_CONFIG_PATH = SCRIPT_DIR / "form_config.json"
DEFAULT_DATASET_KEY = "ucd_icd10_expanded_2018_2024"

# Random delay between browser sessions -- a uniform 6s sleep caused WONDER
# to block the scraper after ~14 hours in earlier testing. CDC documents no
# rate limit anywhere, so this is empirical.
INTER_QUERY_SLEEP_MIN = 20
INTER_QUERY_SLEEP_MAX = 40

# How long wonder_query waits for the CSV to land, in seconds. WONDER's own
# query timeout defaults to 10 minutes; county x race for a big state can
# take a couple of minutes.
DOWNLOAD_MAX_WAIT = 180

# Geographic resolutions this script knows how to loop over. Every query name
# starts with one of these (e.g. "county_age"), and the runner derives both the
# loop shape (county = one query per state, because WONDER caps a query at
# ~75,000 rows and a national county table would be truncated) and the output
# filename from that prefix.
LEVELS = ("national", "state", "county")

# The fixed vocabulary of role names a form_config.json groupby_options block
# is keyed by. "Age" is not here on purpose: it is injected per run from the
# database's age_groupings, see _build_dataset_config.
ROLE_NAMES = ("State", "County", "Year", "Sex", "Race", "Hispanic Origin")

# Role name -> token used in the raw filename. "Sex" maps to "gender" because
# ingest.R and the ~1,800 tracked files under raw/ expect {level}_gender*.csv;
# renaming the file would be a breaking change for no gain.
DEMOGRAPHIC_FILE_TOKENS = {"Sex": "gender", "Race": "race", "Hispanic Origin": "ethnicity"}

# Keys every dataset_versions entry in form_config.json must have. The first
# four are read by the govex package (state_selector is optional there and
# here), the rest by this file. Enforced once, in load_dataset_config.
REQUIRED_DATASET_KEYS = (
    "base_url", "year_selector", "groupby_options",
    "dataset_code", "year_range", "age_groupings", "cause_axes",
)

# Substring WONDER prints in the "Group By:" footer line for each mortality
# age grouping, used by _validate_export to confirm the right one was applied
# (the D158.V5 vs V51 vs V52 confusion is exactly the kind of mistake that
# would otherwise only show up as wrong row counts in ingest.R). Natality's
# mother_* groupings have no entry and are not checked.
AGE_GROUPING_FOOTER_HINTS = {
    "ten_year": "Ten-Year",
    "five_year": "Five-Year",
    "single_year": "Single-Year",
    "infant": "Infant",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Config loading -- the only place form_config.json's schema is enforced
# ─────────────────────────────────────────────────────────────────────────────

def load_dataset_config(config_path=None, dataset_key: str = DEFAULT_DATASET_KEY) -> dict:
    """
    Returns the dataset_versions entry for dataset_key, after checking it has
    every key this script and the govex package will read. A missing key
    would otherwise surface as a KeyError from deep inside a browser session,
    minutes into a run.
    """
    config_path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    full_cfg = load_form_config(str(config_path))
    versions = full_cfg.get("dataset_versions", {})
    base_cfg = versions.get(dataset_key)
    if not base_cfg:
        raise ValueError(
            f"Dataset '{dataset_key}' not found in {config_path}; "
            f"available: {sorted(versions)}"
        )
    missing = [k for k in REQUIRED_DATASET_KEYS if k not in base_cfg]
    if missing:
        raise ValueError(f"form_config.json entry '{dataset_key}' is missing {missing}")
    unknown_roles = [r for r in base_cfg["groupby_options"] if r not in ROLE_NAMES]
    if unknown_roles:
        raise ValueError(
            f"form_config.json entry '{dataset_key}' has groupby_options keys {unknown_roles} "
            f"outside the role vocabulary {list(ROLE_NAMES)} (Age is injected from age_groupings)"
        )
    return base_cfg


# ─────────────────────────────────────────────────────────────────────────────
# Browser setup -- identical to the original drowning_scraper.py, not
# database-specific
# ─────────────────────────────────────────────────────────────────────────────

def setup_browser(headless: bool = True, download_path: str = "./tmp") -> webdriver.Chrome:
    """
    Start a Chrome WebDriver configured for headless operation, automatic
    file downloads to download_path, and stability flags that prevent
    crashes on macOS after many consecutive sessions. Written here rather than
    using the govex package's setup_browser, which lacks the stability flags
    and the CDP download-path call below.
    """
    Path(download_path).mkdir(parents=True, exist_ok=True)

    opts = Options()
    if headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--log-level=3")

    opts.add_experimental_option("prefs", {
        "download.default_directory":   str(Path(download_path).resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade":   True,
        "safebrowsing.enabled":         False,
    })

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)

    # Headless Chrome ignores download.default_directory prefs for
    # auto-downloads. Browser.setDownloadBehavior (CDP) is the reliable modern
    # fix (Page.setDownloadBehavior is deprecated).
    abs_dl = str(Path(download_path).resolve())
    try:
        driver.execute_cdp_cmd("Browser.setDownloadBehavior", {
            "behavior": "allow", "downloadPath": abs_dl, "eventsEnabled": True,
        })
    except Exception as _cdp_err:
        log.warning(f"Browser.setDownloadBehavior failed ({_cdp_err}); falling back")
        try:
            driver.execute_cdp_cmd("Page.setDownloadBehavior", {
                "behavior": "allow", "downloadPath": abs_dl,
            })
        except Exception as _cdp_err2:
            log.warning(f"Page.setDownloadBehavior also failed: {_cdp_err2}")

    return driver


# ─────────────────────────────────────────────────────────────────────────────
# Export options -- guarding against the govex package's blind toggles
# ─────────────────────────────────────────────────────────────────────────────
# After our custom setup step, wonder_query() clicks CO_show_totals,
# CO_show_zeros and CO_show_suppressed once each -- plain .click() calls that
# flip whatever state the box is in, with no look at whether it was already
# checked. On D158 the defaults happen to be totals=on, zeros=off,
# suppressed=off, so the three flips land exactly where we want them (totals
# off, zeros on, suppressed on -- ingest.R relies on suppressed rows being
# present and on a true zero being distinguishable from a missing row). On a
# form with different defaults the same three clicks would invert all three.
# Since the custom setup step runs *before* those clicks, the fix is to set
# each box to the opposite of the desired end state here, so the blind toggle
# always produces the right one. _validate_export then confirms it from the
# export's own "Show ..." footer lines.
EXPORT_OPTION_PRIME = {
    "CO_show_totals":     True,    # -> toggled off
    "CO_show_zeros":      False,   # -> toggled on
    "CO_show_suppressed": False,   # -> toggled on
}


def _prime_export_options(driver) -> None:
    result = driver.execute_script("""
        var want = arguments[0], out = {};
        for (var id in want) {
            var el = document.getElementById(id);
            if (!el) { out[id] = null; continue; }
            el.checked = want[id];
            out[id] = el.checked;
        }
        return out;
    """, EXPORT_OPTION_PRIME)
    for cb_id, state in result.items():
        if state is None:
            # Not fatal here, but wonder_query()'s own .click() on this id
            # will raise and the query will fail -- which is the right outcome
            # (an export with the wrong show-options is worse than no export).
            # If a database genuinely lacks one of these boxes, the documented
            # fallback is to insert a hidden disabled <input> with that id here
            # so the click is harmless; not done pre-emptively.
            log.warning(f"    Export option {cb_id} not on this form; the export step will fail")


# ─────────────────────────────────────────────────────────────────────────────
# Cause-of-death selection
# ─────────────────────────────────────────────────────────────────────────────
# Two approaches were tried here. WONDER's finder widget exposes a text
# search box (TD158.V2 + finder-action-D158.V2-Search) that looked like it
# would generalize better than tree-clicking, since it wouldn't require
# knowing where a code sits in the tree. It didn't work out: send_keys()
# raised ElementNotInteractableException on the live textarea; a JS-based
# workaround fixed that but then "W65-W74" returned zero matches (the
# widget turned out to be a multi-term boolean search, not a code-range
# parser); and after searching, the results select's DOM element gets
# replaced entirely, and where the results actually land was not run down
# before this was abandoned in favor of the approach below.
#
# What's used instead is drowning_scraper.py's original, PROVEN mechanism:
# click through the ICD Browse tree (find `tree_path[0]` and "Open Fully"
# to expand it, revealing its children; repeat for each subsequent
# tree_path entry; finally find and select `codes` itself and "Open Fully"
# once more to apply it). This ran successfully for this entire project's
# drowning pull (700+ live queries), so the click mechanics themselves are
# trustworthy -- generalizing it just means the caller supplies which
# branches to expand, instead of them being hardcoded to drowning's
# "V01-Y89" -> "W65-W74".
#
# Generalizing across *databases* turned out to need only one more thing:
# every id of the finder widget is derived from a single WONDER variable
# code -- list codes-<var>, buttons finder-action-<var>-Open Fully / -Open,
# current-filter textarea <var>-fhi, and the radio RO_ucd<var> / RO_mcd<var>
# that makes this finder the one the form submits. The original hardcoded
# all of them to D158.V2; here the variable comes from the database's
# cause_axes entry in form_config.json, looked up when the callback runs
# (it receives dataset_config from the govex package). The radio matters on
# the Multiple Cause databases, where the underlying-cause and
# multiple-cause finders both sit in the DOM and only the radio-selected one
# is submitted; on D158 the only radio is already selected and the click is
# a no-op.
#
# Finding tree_path for a new cause of death requires a one-time manual
# step: load the database's request form, find the ICD code finder, and note
# which top-level chapter (and any sub-branches) you have to expand to reach
# it. This is the same kind of one-time discovery this project already did
# for the variable codes in form_config.json -- not written down anywhere by
# CDC, but stable once found.

def make_cause_selector(codes: str, tree_path: list = (), axis: str = "underlying"):
    """
    Returns the callback the govex package calls as:
        custom_setup_func(driver, dataset_config, query_name, year)

    codes     -- exact visible text of the ICD code/range to select, e.g.
                 "W65-W74" (must match how it's labeled in the tree; the
                 lookup is a substring match, so "V01" would also match
                 "V01-Y89" -- be as specific as the tree label is).
    tree_path -- parent branch(es) to expand, in order, to reveal codes.
                 For drowning this is ["V01-Y89"] (the injury-causes
                 chapter). Leave empty only if codes is itself a
                 top-level entry.
    axis      -- which of the database's cause_axes to filter through:
                 "underlying" on every mortality database, "multiple" on the
                 Multiple Cause of Death ones (a death counts if the code
                 appears anywhere on the certificate).
    """

    def _inject(driver, dataset_config, query_name, year):
        axes = dataset_config.get("cause_axes", {})
        if axis not in axes:
            log.error(f"    cause axis {axis!r} not offered by this database (has {sorted(axes)})")
            return False
        var = axes[axis]
        list_id = f"codes-{var}"
        fhi_id = f"{var}-fhi"
        log.info(f"    Selecting ICD codes {codes!r} via {var} Browse tree (path: {list(tree_path)}) ...")

        def scroll_to(el):
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.4)

        def safe_click(el):
            try:
                el.click()
            except Exception:
                driver.execute_script("arguments[0].click();", el)

        def select_finder_radio():
            # RO_ucd<var> on the underlying-cause axis, RO_mcd<var> on the
            # multiple-cause axis. Whichever exists is the right one.
            for radio_id in (f"RO_ucd{var}", f"RO_mcd{var}"):
                try:
                    radio = driver.find_element(By.ID, radio_id)
                except Exception:
                    continue
                if not radio.is_selected():
                    scroll_to(radio)
                    safe_click(radio)
                    time.sleep(1)
                    log.info(f"    Selected finder radio {radio_id}")
                return True
            log.warning(f"    No RO_ucd/RO_mcd radio found for {var}; assuming its finder is already active")
            return False

        def click_open_fully():
            for btn_name in [f"finder-action-{var}-Open Fully", f"finder-action-{var}-Open"]:
                try:
                    btn = driver.find_element(By.NAME, btn_name)
                    scroll_to(btn)
                    safe_click(btn)
                    time.sleep(2)
                    log.info(f"    Clicked '{btn_name}'")
                    return True
                except Exception:
                    pass
            log.warning("    Open Fully / Open button not found")
            return False

        def find_tree_item(text):
            # The tree is a custom JS widget backed by a <select multiple>, so
            # search by text content across element types, list first.
            xpaths = [
                f"//*[@id='{list_id}']/descendant::*[contains(.,'{text}')]",
                f"//option[contains(.,'{text}')]",
                f"//*[contains(text(),'{text}') and not(descendant::*[contains(text(),'{text}')])]",
            ]
            for xp in xpaths:
                try:
                    for el in driver.find_elements(By.XPATH, xp):
                        try:
                            if el.is_displayed():
                                return el
                        except Exception:
                            pass
                except Exception:
                    pass
            return None

        def js_deselect_all():
            # Every click ADDS to the multi-select; clear it before each
            # meaningful click so only ONE item is selected when "Open Fully"
            # fires (WONDER reads the currently-selected options).
            driver.execute_script("""
                var sel = document.getElementById(arguments[0]);
                for (var i=0; i<sel.options.length; i++) sel.options[i].selected = false;
            """, list_id)
            time.sleep(0.3)

        # If this returns False, the govex package skips saving this query's
        # result entirely (per its contract) -- used on every failure path
        # below instead of returning True, so a filter that didn't apply can
        # never result in an unfiltered (all-cause) file being downloaded and
        # mistaken for real data. This was a real bug caught while testing
        # the search-box approach above: it silently downloaded all-cause
        # mortality (crude rate ~700/100k, vs. drowning's ~1/100k) the first
        # time, because an earlier version of this function always returned
        # True even on failure -- a pattern the original drowning_scraper.py
        # selector also had, just never hit in practice there.
        try:
            _prime_export_options(driver)
            select_finder_radio()

            wait = WebDriverWait(driver, 20)
            wait.until(EC.presence_of_element_located((By.ID, list_id)))
            time.sleep(2)  # let the JS tree widget finish initializing
            scroll_to(driver.find_element(By.ID, list_id))

            js_deselect_all()  # remove default *All*
            remaining = list(tree_path) + [codes]
            for depth, branch in enumerate(tree_path):
                log.info(f"    Looking for {branch!r} in Browse tree ...")
                el = find_tree_item(branch)
                if not el:
                    log.error(f"    {branch!r} not found in ICD tree")
                    return False
                scroll_to(el)
                safe_click(el)
                time.sleep(0.5)
                log.info(f"    Expanding {branch!r} ...")
                click_open_fully()
                next_target = remaining[depth + 1]
                try:
                    WebDriverWait(driver, 15).until(lambda d: find_tree_item(next_target) is not None)
                except Exception:
                    log.warning(f"    {next_target!r} did not appear after expanding {branch!r}; proceeding anyway")
                js_deselect_all()  # remove this branch before descending/selecting further

            log.info(f"    Looking for {codes!r} ...")
            target = find_tree_item(codes)
            if not target:
                log.error(f"    {codes!r} not found (check tree_path is correct)")
                return False
            scroll_to(target)
            safe_click(target)
            time.sleep(0.5)
            log.info(f"    Applying {codes!r} via Open Fully ...")
            if not click_open_fully():
                return False

            # What the form will submit as the filter is the set of highlighted
            # options in the codes-<var> select (name F_<var>); read that
            # directly. The regular finder (the UCD axes) also has a read-only
            # <var>-fhi textarea mirroring the highlight, which is what the
            # original drowning scraper checked -- but the Multiple Cause
            # finder runs in WONDER's "advanced" mode (O_Vxx_fmode=fadv, with
            # AND1/AND2 "Selected Items" boxes) and has no -fhi element at all;
            # the first D157 smoke test failed on exactly that lookup.
            applied = driver.execute_script("""
                var sel = document.getElementById(arguments[0]);
                var out = [];
                for (var i = 0; i < sel.options.length; i++)
                    if (sel.options[i].selected) out.push(sel.options[i].text.trim());
                return out.join('; ');
            """, list_id)
            try:
                fhi_val = driver.find_element(By.ID, fhi_id).get_attribute("value") or ""
                log.info(f"    ICD fhi: {fhi_val[:150]!r}")
            except Exception:
                fhi_val = ""
            log.info(f"    Highlighted in {list_id}: {applied[:150]!r}")
            # "*All* (All Causes of Death)" is the unfiltered state; test the
            # prefix since the wording differs a little between finders. And
            # the highlight must actually name what was asked for.
            if not applied or applied.startswith("*All*") or codes not in applied:
                log.error(f"    FILTER NOT APPLIED (highlight is {applied[:80]!r}) — "
                          "refusing to proceed with an unfiltered query")
                return False

            # Advanced-mode finders need one more step. On the regular finder
            # the highlighted options ARE the submitted filter. On an advanced
            # one (the Multiple Cause axis; recognizable by its T<var>-AND1
            # "Selected Items" textarea) the server reads that textarea
            # (V_<var>) instead, and a highlight that was never moved into it
            # is ignored: the first D157 smoke test highlighted W65-W74
            # correctly and still came back as an all-cause export (crude
            # rate ~1,000/100k), caught only by the footer validation. The
            # move is what the finder's arrow button does -- WONDER's own
            # add() in /finder.js, which appends each highlighted option's
            # code label to the textarea -- so call exactly that.
            adv_box_id = f"T{var}-AND1"
            try:
                adv_box = driver.find_element(By.ID, adv_box_id)
            except Exception:
                adv_box = None
            if adv_box is not None:
                driver.execute_script("add(arguments[0], 'and2');", var)
                time.sleep(0.5)
                moved = adv_box.get_attribute("value") or ""
                log.info(f"    Selected Items box {adv_box_id}: {moved.strip()[:150]!r}")
                if codes not in moved:
                    log.error(f"    FILTER NOT APPLIED (Selected Items box is {moved[:80]!r}) — "
                              "refusing to proceed with an unfiltered query")
                    return False

            log.info(f"    ICD codes {codes!r} applied ✓")
            return True

        except Exception as exc:
            log.error(f"    ICD injection failed: {exc} -- refusing to proceed unfiltered")
            return False

    return _inject


# Kept so an older notebook/import keeps working; new code should call
# make_cause_selector, which this is now just a name for.
def make_icd_selector(icd_codes: str, tree_path: list = ()):
    return make_cause_selector(icd_codes, tree_path, axis="underlying")


def make_setup_func(cause_selector=None):
    """
    The callback handed to wonder_query(). With a cause selector it is that
    selector (which primes the export options itself, first thing). Without
    one -- natality, or an explicit all-causes pull -- it is the export-option
    priming alone, so that guard still runs on every query.
    """
    if cause_selector is not None:
        return cause_selector

    def _prime_only(driver, dataset_config, query_name, year):
        _prime_export_options(driver)
        return True

    return _prime_only


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic query config -- builds the dict shape the govex package expects
# from form_config.json's entry plus the caller's parameters
# ─────────────────────────────────────────────────────────────────────────────

def _build_dataset_config(base_cfg: dict, age_grouping: str, year_range: Optional[tuple] = None,
                          groupby_overrides: Optional[dict] = None) -> dict:
    """
    Copies the database entry and injects the run-specific parts: every age
    grouping the database offers becomes a group-by label "Age[<name>]", and
    the selected one is also aliased to plain "Age". That is what lets one
    run mix resolutions -- the drowning pull needs County x Ten-Year for
    deaths and County x Five-Year for population (see ingest.R's
    FIVE_TO_TEN_YEAR_AGE) -- without the runner knowing anything about ages.
    groupby_overrides swaps a role's code for this run (e.g. bridged race
    D66.V2 in place of single race D66.V42 on natality_2007_2024).
    """
    age_codes = base_cfg["age_groupings"]
    if age_grouping not in age_codes:
        raise ValueError(
            f"age_grouping must be one of {sorted(age_codes)} for this database, got {age_grouping!r}"
        )

    ds_start, ds_end = base_cfg["year_range"]
    if year_range is None:
        year_range = (ds_start, ds_end)
    yr_start, yr_end = year_range
    if yr_start < ds_start or yr_end > ds_end or yr_start > yr_end:
        raise ValueError(
            f"year_range {tuple(year_range)} falls outside this database's range "
            f"({ds_start}-{ds_end}); another range lives in a different WONDER database "
            "with its own entry in form_config.json."
        )

    cfg = dict(base_cfg)
    cfg["groupby_options"] = dict(base_cfg["groupby_options"])
    for name, code in age_codes.items():
        cfg["groupby_options"][f"Age[{name}]"] = code
    cfg["groupby_options"]["Age"] = age_codes[age_grouping]
    cfg["age_grouping"] = age_grouping
    for role, code in (groupby_overrides or {}).items():
        if role not in cfg["groupby_options"]:
            raise ValueError(f"groupby_overrides role {role!r} is not a known group-by label")
        cfg["groupby_options"][role] = code
    cfg["year_range"] = [yr_start, yr_end]
    return cfg


def _level_of(qname: str) -> str:
    level = qname.split("_", 1)[0]
    if level not in LEVELS:
        raise ValueError(
            f"query name {qname!r} must start with one of {LEVELS} followed by '_' "
            "(the prefix decides the loop shape and the filename)"
        )
    return level


def build_queries(geography_levels, stratify_sex: bool = True, stratify_race: bool = True,
                  extra_queries: Optional[dict] = None) -> dict:
    """
    Returns the {query_name: [group-by labels]} dict the govex package reads,
    in the naming scheme raw/ and ingest.R already use:

        national_total     ["Year"]              national: no geo group-by, no
        national_age       ["Age"]                state filter. WONDER needs at
        national_gender    ["Sex"]                least one group-by, and Year
        national_race      ["Race"]               with a single-year filter
        national_ethnicity ["Hispanic Origin"]    yields the one-row plain total
        state_age          ["State", "Age"]      state: one query per year
        state_gender       ["State", "Sex"]
        ...
        county_age         ["County", "Age"]     county: one query per state
        county_gender      ["County", "Sex"]     per year

    Race and Hispanic origin are queried separately, not cross-tabulated with
    each other or with age/sex -- the design the drowning pipeline uses.
    extra_queries is merged last, e.g. {"county_pop5yr": ["County",
    "Age[five_year]"]}; names must carry a level prefix like the built-ins.
    """
    queries = {}
    for level in geography_levels:
        if level not in LEVELS:
            raise ValueError(f"geography_levels entries must be one of {LEVELS}, got {level!r}")
        geo = [] if level == "national" else [level.capitalize()]
        if level == "national":
            queries["national_total"] = ["Year"]
        queries[f"{level}_age"] = geo + ["Age"]
        if stratify_sex:
            queries[f"{level}_{DEMOGRAPHIC_FILE_TOKENS['Sex']}"] = geo + ["Sex"]
        if stratify_race:
            queries[f"{level}_{DEMOGRAPHIC_FILE_TOKENS['Race']}"] = geo + ["Race"]
            queries[f"{level}_{DEMOGRAPHIC_FILE_TOKENS['Hispanic Origin']}"] = geo + ["Hispanic Origin"]
    for name, labels in (extra_queries or {}).items():
        _level_of(name)
        queries[name] = list(labels)
    return queries


def _validate_queries(dataset_cfg: dict) -> None:
    # Every label must resolve to a WONDER code now, not minutes into a
    # browser session as a KeyError inside the govex package.
    known = dataset_cfg["groupby_options"]
    for name, labels in dataset_cfg["queries"].items():
        if len(labels) > 5:
            raise ValueError(f"query {name!r} has {len(labels)} group-bys; WONDER forms take at most 5")
        for label in labels:
            if label not in known:
                raise ValueError(
                    f"query {name!r} uses group-by {label!r}, which this database does not define "
                    f"(known: {sorted(known)})"
                )


# ─────────────────────────────────────────────────────────────────────────────
# Expected-file enumeration -- one function feeds both the runner and --check
# ─────────────────────────────────────────────────────────────────────────────
# The two Nevada files missing from raw/ (2023 county_race, 2024
# county_pop5yr) went unnoticed for weeks because "no file" was one warning
# line in a 700-line log, and ingest.R then quietly absorbed the gap as
# missing_denom_flag = 1. Deriving "what should exist" and "what a run will
# pull" from the same list makes the gap reportable offline and makes a
# re-run fill exactly it.

PlannedQuery = namedtuple("PlannedQuery", ["dest", "qname", "year", "state"])


def expected_files(output_dir, queries: dict, years, states=US_STATES) -> list:
    """
    Every file a full run of `queries` over `years` produces, in run order:
    output_dir/<year>/<qname>.csv for state and national queries,
    output_dir/<year>/<qname>_<State_Name>.csv for county queries.
    """
    output_dir = Path(output_dir)
    planned = []
    for year in years:
        year_dir = output_dir / str(year)
        for qname in queries:
            if _level_of(qname) == "county":
                for state in states:
                    planned.append(PlannedQuery(year_dir / f"{qname}_{state.replace(' ', '_')}.csv",
                                                qname, year, state))
            else:
                planned.append(PlannedQuery(year_dir / f"{qname}.csv", qname, year, None))
    return planned


# Every WONDER export ends with a "Query Parameters:" block that restates the
# request -- cause codes, state filter, year, group-bys, show-options. It is
# the cheapest possible check that a file is what its name claims, and it is
# free: no browser, no network.
_FOOTER_KEY_RE = re.compile(r'^([A-Z][A-Za-z0-9 /\-]{0,40}): (.*)$')

# How the mortality databases word the footer. A form_config.json entry can
# override any of these under "footer" (natality needs to: its geography
# labels are "State of Residence" / "County of Residence").
FOOTER_DEFAULTS = {
    "year_key": "Year/Month",
    "state_key": "States",
    "geo_labels": {"state": "State", "county": "County"},
}

# Where a download that failed validation is kept for inspection instead of
# being thrown away with its temp dir -- outside the repo, so it can never be
# mistaken for real data or picked up by ingest.R's hash of raw/.
REJECTED_DIR = Path(tempfile.gettempdir()) / "cdc_wonder_rejected"


def _parse_query_footer(path) -> dict:
    """
    Returns {key: value} for the lines between "Query Parameters:" and the
    next "---" separator, joining WONDER's wrapped continuation lines (the
    ICD code list wraps over several quoted lines) back onto their key.
    Empty dict if the file has no such block -- e.g. a truncated download.
    """
    params = {}
    in_block = False
    current_key = None
    with open(path, encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            line = raw.rstrip("\r\n")
            if line.startswith('"') and line.endswith('"'):
                line = line[1:-1]
            if not in_block:
                if line == "Query Parameters:":
                    in_block = True
                continue
            if line == "---":
                break
            m = _FOOTER_KEY_RE.match(line)
            if m:
                current_key = m.group(1)
                params[current_key] = m.group(2)
            elif current_key is not None:
                params[current_key] += " " + line
    return params


def _validate_export(path, planned: PlannedQuery, dataset_cfg: dict,
                     cause_codes: Optional[str]) -> list:
    """
    Compares one export's footer against what its query should have asked
    for. Returns a list of problems (empty = fine). Used both by --check
    --verify on the tracked files and by the runner on each fresh download
    before it is moved into raw/.
    """
    problems = []
    params = _parse_query_footer(path)
    if not params:
        return ["no 'Query Parameters:' footer (truncated or not a WONDER export)"]

    labels = dataset_cfg["queries"][planned.qname]
    level = _level_of(planned.qname)
    # The footer's wording differs between database families (natality says
    # "State of Residence" where mortality says "State"); a database entry
    # may override any of these under a "footer" key.
    footer = dict(FOOTER_DEFAULTS)
    footer.update(dataset_cfg.get("footer", {}))
    year_key, state_key = footer["year_key"], footer["state_key"]
    geo_labels = footer["geo_labels"]

    if params.get(year_key) != str(planned.year):
        problems.append(f"{year_key} is {params.get(year_key)!r}, expected {planned.year}")

    states_line = params.get(state_key)
    if planned.state is not None:
        if not states_line or planned.state not in states_line:
            problems.append(f"{state_key} is {states_line!r}, expected {planned.state!r}")
    elif states_line:
        problems.append(f"unexpected state filter {states_line!r} on a {level} query")

    group_by = [g.strip() for g in params.get("Group By", "").split(";") if g.strip()]
    if len(group_by) != len(labels):
        problems.append(f"Group By is {group_by}, expected {len(labels)} dimension(s) for {labels}")
    elif level in ("state", "county"):
        if group_by[0] != geo_labels[level]:
            problems.append(f"Group By starts with {group_by[0]!r}, expected {geo_labels[level]!r}")
    elif group_by and group_by[0] in geo_labels.values():
        problems.append(f"national query grouped by {group_by[0]!r}")
    for label in labels:
        if label.startswith("Age"):
            grouping = label[4:-1] if label.startswith("Age[") else dataset_cfg.get("age_grouping")
            hint = AGE_GROUPING_FOOTER_HINTS.get(grouping)
            if hint and not any(hint in g for g in group_by):
                problems.append(f"Group By {group_by} does not mention {hint!r} for age grouping {grouping!r}")

    # "Show Totals" prints as "Disabled" on state-level exports and "False" on
    # county-level ones (both seen in raw/); either means off.
    if params.get("Show Totals") not in ("False", "Disabled"):
        problems.append(f"Show Totals is {params.get('Show Totals')!r}, expected off")
    for key in ("Show Zero Values", "Show Suppressed"):
        if params.get(key) != "True":
            problems.append(f"{key} is {params.get(key)!r}, expected True")

    if cause_codes:
        code_lines = [v for k, v in params.items() if k.endswith("ICD-10 Codes")]
        first_code = cause_codes.split("-")[0].strip()
        if not code_lines:
            problems.append("no ICD-10 Codes line -- an unfiltered all-cause export?")
        elif not any(first_code in v for v in code_lines):
            problems.append(f"ICD-10 Codes line does not mention {first_code!r}")
    return problems


def check_missing(output_dir, queries: dict, years, states=US_STATES, only=None,
                  verify: bool = False, dataset_cfg: Optional[dict] = None,
                  cause_codes: Optional[str] = None) -> dict:
    """
    Diffs the expected file set against disk. Returns
        {"expected": n, "present": [...], "missing": [...],
         "unexpected": [...], "invalid": [(path, [problems]), ...]}
    where unexpected = csv files under output_dir/<year>/ that no query in
    `queries` produces for any state (stale names from an older config), and
    invalid is filled only with verify=True (needs dataset_cfg for the
    group-by labels). `states` and `only` narrow what is reported present /
    missing / invalid -- the same narrowing a run with --states / --only
    applies -- but NOT the unexpected check, or every other state's files
    would be flagged as strays.
    """
    output_dir = Path(output_dir)
    planned_all = expected_files(output_dir, queries, years, US_STATES)
    planned = [p for p in planned_all
               if (p.state is None or p.state in states) and (not only or p.qname in only)]
    present = [p for p in planned if p.dest.exists()]
    missing = [p for p in planned if not p.dest.exists()]
    expected_paths = {p.dest for p in planned_all}
    unexpected = []
    for year in years:
        year_dir = output_dir / str(year)
        if year_dir.is_dir():
            unexpected += sorted(f for f in year_dir.glob("*.csv") if f not in expected_paths)
    invalid = []
    if verify:
        if dataset_cfg is None:
            raise ValueError("verify=True needs dataset_cfg")
        for p in present:
            problems = _validate_export(p.dest, p, dataset_cfg, cause_codes)
            if problems:
                invalid.append((p.dest, problems))
    return {
        "expected": len(planned), "present": present, "missing": missing,
        "unexpected": unexpected, "invalid": invalid,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Query runner
# ─────────────────────────────────────────────────────────────────────────────

def _run_queries(output_dir, dataset_cfg: dict, years, setup_func, headless: bool,
                 states=US_STATES, only=None, cause_codes: Optional[str] = None) -> dict:
    """
    Walks expected_files() in order, skipping files already on disk, and runs
    one fresh browser session per remaining query. Returns
    {"downloaded": [...], "skipped": [...], "failed": [(PlannedQuery, reason)]}.
    """
    output_dir = Path(output_dir)
    downloaded, skipped, failed = [], [], []

    for planned in expected_files(output_dir, dataset_cfg["queries"], years, states):
        if only and planned.qname not in only:
            continue
        if planned.dest.exists():
            log.info(f"  SKIP (exists): {planned.year}/{planned.dest.name}")
            skipped.append(planned.dest)
            continue

        where = planned.state or ("all states" if _level_of(planned.qname) == "state" else "national")
        log.info(f"  {planned.qname} | {where} | {planned.year}")

        # A fresh temp dir per query, outside the repo. Three independent
        # reasons, each sufficient on its own: wonder_query() returns the
        # newest *.csv already sitting in its download dir, so a leftover from
        # a previous query would be returned instantly as this one's result;
        # ingest.R md5-hashes every csv under raw/ (recursively) to decide
        # whether to rebuild, so a temp file there would change the process
        # state; and the old raw/<dataset>/<year>/tmp/ layout left empty
        # directories behind.
        tmp = Path(tempfile.mkdtemp(prefix="cdc_wonder_"))
        driver = setup_browser(headless=headless, download_path=str(tmp))
        try:
            ok, path = wonder_query(
                driver=driver, dataset_config=dataset_cfg, query_name=planned.qname,
                year=planned.year, download_path=str(tmp), state_name=planned.state,
                custom_setup_func=setup_func, max_wait=DOWNLOAD_MAX_WAIT,
            )
            if not (ok and path):
                log.warning(f"    ✗ No file: {planned.qname}/{where}/{planned.year}")
                failed.append((planned, "no file downloaded"))
            else:
                problems = _validate_export(path, planned, dataset_cfg, cause_codes)
                if problems:
                    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
                    kept = REJECTED_DIR / f"{planned.year}_{planned.dest.name}"
                    shutil.move(str(path), str(kept))
                    log.error(f"    ✗ Export rejected for {planned.dest.name}: " + "; ".join(problems))
                    log.error(f"      kept for inspection at {kept}; its footer said: "
                              f"{_parse_query_footer(kept)}")
                    failed.append((planned, "; ".join(problems)))
                else:
                    planned.dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(path), str(planned.dest))
                    log.info(f"    ✓ {planned.year}/{planned.dest.name}")
                    downloaded.append(planned.dest)
        except Exception as exc:
            log.error(f"    ERROR: {exc}")
            failed.append((planned, str(exc)))
        finally:
            driver.quit()
            shutil.rmtree(tmp, ignore_errors=True)

        _sleep = random.uniform(INTER_QUERY_SLEEP_MIN, INTER_QUERY_SLEEP_MAX)
        log.info(f"    Sleeping {_sleep:.0f}s before next query ...")
        time.sleep(_sleep)

    # The end-of-run tally is the whole reason the Nevada gaps would be
    # caught today: a failure is one line here, not one line somewhere in
    # the middle of hundreds.
    log.info(f"\n>>> {len(downloaded)} downloaded, {len(skipped)} skipped (already present), "
             f"{len(failed)} FAILED")
    for planned, reason in failed:
        log.error(f"    FAILED {planned.year}/{planned.dest.name}: {reason}")
    return {"downloaded": downloaded, "skipped": skipped, "failed": failed}


# ─────────────────────────────────────────────────────────────────────────────
# Public entry points
# ─────────────────────────────────────────────────────────────────────────────

def _prepare(dataset_key, codes, tree_path, cause_axis, allow_all_causes, year_range,
             geography_levels, age_grouping, stratify_sex, stratify_race, extra_queries,
             groupby_overrides, output_dir, config_path):
    """Everything pull_cdc_wonder and the --check path share: config, queries, paths."""
    base_cfg = load_dataset_config(config_path, dataset_key)

    # Cause-of-death guard rails. A mortality database with no filter is an
    # all-cause pull -- almost never what anyone wants, and exactly the
    # silent-wrong-data failure this project already hit once -- so it has to
    # be asked for by name. Natality has no cause finder at all.
    has_cause_axes = bool(base_cfg["cause_axes"])
    if codes and not has_cause_axes:
        raise ValueError(f"'{dataset_key}' has no cause-of-death finder; drop the codes argument")
    if not codes and has_cause_axes and not allow_all_causes:
        raise ValueError(
            f"'{dataset_key}' is a mortality database and no codes were given; this would pull "
            "ALL causes of death. Pass allow_all_causes=True (--allow-all-causes) if that is intended."
        )
    if codes and cause_axis not in base_cfg["cause_axes"]:
        raise ValueError(
            f"cause_axis {cause_axis!r} not offered by '{dataset_key}' "
            f"(has {sorted(base_cfg['cause_axes'])})"
        )

    dataset_cfg = _build_dataset_config(base_cfg, age_grouping, year_range, groupby_overrides)
    dataset_cfg["queries"] = build_queries(list(geography_levels), stratify_sex, stratify_race,
                                           extra_queries)
    _validate_queries(dataset_cfg)
    years = list(range(dataset_cfg["year_range"][0], dataset_cfg["year_range"][1] + 1))
    output_dir = Path(output_dir) if output_dir else RAW_ROOT / dataset_key
    return dataset_cfg, years, output_dir


def pull_cdc_wonder(
    dataset_key: str,
    codes: Optional[str] = None,
    tree_path: list = (),
    cause_axis: str = "underlying",
    allow_all_causes: bool = False,
    year_range: Optional[tuple] = None,
    geography_levels=("state",),
    age_grouping: str = "ten_year",
    stratify_sex: bool = True,
    stratify_race: bool = True,
    extra_queries: Optional[dict] = None,
    groupby_overrides: Optional[dict] = None,
    output_dir=None,
    states=None,
    only=None,
    config_path=None,
    headless: bool = True,
) -> dict:
    """
    Pulls one cause (or, for natality, all births) from one CDC WONDER
    database, one CSV per year x geography-level (x state, for county) x
    demographic breakdown. Returns {"downloaded", "skipped", "failed"} (see
    _run_queries). Already-present files are skipped, never re-downloaded.

    dataset_key       -- entry of form_config.json's dataset_versions.
    codes             -- exact visible text of the ICD code/range in WONDER's
                         Browse tree, e.g. "W65-W74". Required on mortality
                         databases unless allow_all_causes; forbidden on
                         natality.
    tree_path         -- parent branch(es) to expand to reveal codes, e.g.
                         ["V01-Y89"] for drowning; see make_cause_selector.
    cause_axis        -- "underlying" (default) or "multiple" (Multiple Cause
                         databases only).
    allow_all_causes  -- explicitly pull all causes of death on a mortality
                         database (no ICD filter at all).
    year_range        -- (start, end) inclusive, within the database's range;
                         None = the whole range.
    geography_levels  -- any of "national", "state", "county". County loops
                         all 51 states per year (WONDER's ~75,000-row cap
                         would silently truncate a national county table).
    age_grouping      -- a name from the database's age_groupings: ten_year /
                         five_year / single_year / infant on mortality,
                         mother_9 / mother_10 / mother_13 on natality. Note the
                         county-level gap found while building the drowning
                         pipeline: County x Ten-Year Age Groups on D158
                         returns Deaths only, no Population/Crude Rate, for
                         every state and year -- a real CDC WONDER limitation,
                         not a bug here. It likely reproduces for any cause of
                         death; see ingest.R's build_county_age() for the
                         Five-Year-derived population workaround, and
                         extra_queries below for how to pull that file.
    stratify_sex      -- also pull a Sex-only breakdown.
    stratify_race     -- also pull Race-only and Hispanic-Origin-only
                         breakdowns (queried separately, not cross-tabulated).
    extra_queries     -- {name: [group-by labels]} merged into the built-in
                         set, e.g. {"county_pop5yr": ["County", "Age[five_year]"]}.
    groupby_overrides -- {role: code} to swap a variable for this run.
    output_dir        -- defaults to raw/<dataset_key>/ in this repo. Pass
                         your own for a second cause on the same database.
    states            -- subset of state names for county queries (default all).
    only              -- subset of query names to run (default all).
    config_path       -- path to form_config.json (default: next to this file).
    headless          -- run Chrome headless (default) or visibly.

    Keep the machine awake for the duration -- see the module docstring.
    """
    dataset_cfg, years, output_dir = _prepare(
        dataset_key, codes, tree_path, cause_axis, allow_all_causes, year_range,
        geography_levels, age_grouping, stratify_sex, stratify_race, extra_queries,
        groupby_overrides, output_dir, config_path,
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Pulling {dataset_key} ({dataset_cfg['dataset_code']}), "
             f"codes={codes!r} axis={cause_axis if codes else '-'}, "
             f"{len(years)} years ({years[0]}-{years[-1]}), geography={list(geography_levels)}, "
             f"age={age_grouping}, sex={'yes' if stratify_sex else 'no'}, "
             f"race={'yes' if stratify_race else 'no'}")
    log.info(f"Queries: {list(dataset_cfg['queries'])}")
    log.info(f"Output: {output_dir}")

    cause_selector = make_cause_selector(codes, tree_path, cause_axis) if codes else None
    setup_func = make_setup_func(cause_selector)
    result = _run_queries(output_dir, dataset_cfg, years, setup_func, headless,
                          states=list(states) if states else US_STATES, only=only,
                          cause_codes=codes)
    return result


# The pre-generalization entry point, kept as a thin shim. Returns the list
# of files present after the run (downloaded + skipped), as it always did.
def pull_cdc_wonder_ucd(icd_codes, output_dir, tree_path=(), year_range=(2018, 2024),
                        geography_levels=("state",), age_grouping="ten_year",
                        stratify_sex=True, stratify_race=True, config_path=None,
                        dataset_key=DEFAULT_DATASET_KEY, headless=True) -> list:
    result = pull_cdc_wonder(
        dataset_key, codes=icd_codes, tree_path=tree_path, year_range=year_range,
        geography_levels=geography_levels, age_grouping=age_grouping,
        stratify_sex=stratify_sex, stratify_race=stratify_race,
        output_dir=output_dir, config_path=config_path, headless=headless,
    )
    return result["downloaded"] + result["skipped"]


# ─────────────────────────────────────────────────────────────────────────────
# Command line
# ─────────────────────────────────────────────────────────────────────────────

def _parse_extra_query(text: str) -> tuple:
    # "county_pop5yr=County,Age[five_year]" -> ("county_pop5yr", ["County", "Age[five_year]"])
    if "=" not in text:
        raise argparse.ArgumentTypeError(f"--extra-query needs NAME=Label,Label, got {text!r}")
    name, labels = text.split("=", 1)
    return name.strip(), [lab.strip() for lab in labels.split(",") if lab.strip()]


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Pull one cause of death (or births) from a CDC WONDER database into raw/.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--dataset", required=True, metavar="KEY",
                   help="entry of form_config.json's dataset_versions")
    p.add_argument("--codes", metavar="TEXT",
                   help="exact ICD code/range label in WONDER's Browse tree, e.g. W65-W74 "
                        "(omit on natality)")
    p.add_argument("--tree-path", nargs="*", default=[], metavar="BRANCH",
                   help="branch(es) to expand to reach --codes, e.g. V01-Y89")
    p.add_argument("--cause-axis", default="underlying", choices=("underlying", "multiple"))
    p.add_argument("--allow-all-causes", action="store_true",
                   help="pull ALL causes of death (no ICD filter) on a mortality database")
    p.add_argument("--years", nargs=2, type=int, metavar=("START", "END"),
                   help="inclusive; default is the database's whole range")
    p.add_argument("--levels", nargs="+", default=["state"], choices=LEVELS)
    p.add_argument("--age", default="ten_year", metavar="NAME",
                   help="age grouping name from the database's age_groupings (default ten_year)")
    p.add_argument("--extra-query", action="append", default=[], type=_parse_extra_query,
                   metavar="NAME=Label,Label",
                   help="additional query, e.g. county_pop5yr=County,Age[five_year]; repeatable")
    p.add_argument("--no-sex", action="store_true", help="skip the Sex breakdown")
    p.add_argument("--no-race", action="store_true", help="skip the Race and Hispanic Origin breakdowns")
    p.add_argument("--output-dir", metavar="PATH", help="default raw/<dataset key>/ in this repo")
    p.add_argument("--states", nargs="+", metavar="NAME",
                   help="only these states for county queries (default all 50 + DC)")
    p.add_argument("--only", nargs="+", metavar="QNAME", help="only these query names")
    p.add_argument("--check", action="store_true",
                   help="report expected-vs-present files and download nothing")
    p.add_argument("--verify", action="store_true",
                   help="with --check: also parse each present file's footer and flag mismatches")
    p.add_argument("--no-headless", action="store_true", help="show the browser")
    p.add_argument("--config", metavar="PATH", help="form_config.json (default: next to this file)")
    return p


def main(argv=None) -> int:
    args = build_arg_parser().parse_args(argv)
    extra = dict(args.extra_query) if args.extra_query else None
    year_range = tuple(args.years) if args.years else None

    if args.check:
        dataset_cfg, years, output_dir = _prepare(
            args.dataset, args.codes, args.tree_path, args.cause_axis, args.allow_all_causes,
            year_range, args.levels, args.age, not args.no_sex, not args.no_race, extra,
            None, args.output_dir, args.config,
        )
        report = check_missing(output_dir, dataset_cfg["queries"], years,
                               states=args.states or US_STATES, only=args.only,
                               verify=args.verify, dataset_cfg=dataset_cfg, cause_codes=args.codes)
        print(f"{output_dir}: {len(report['present'])}/{report['expected']} expected files present")
        if report["missing"]:
            print(f"MISSING ({len(report['missing'])}):")
            for p in report["missing"]:
                print(f"  {p.year}/{p.dest.name}")
        if report["unexpected"]:
            print(f"UNEXPECTED ({len(report['unexpected'])}) -- csv files no current query produces:")
            for f in report["unexpected"]:
                print(f"  {f.parent.name}/{f.name}")
        if args.verify:
            print(f"verified footers of {len(report['present'])} files: {len(report['invalid'])} invalid")
            for f, problems in report["invalid"]:
                print(f"  {f.parent.name}/{f.name}: " + "; ".join(problems))
        return 1 if (report["missing"] or report["unexpected"] or report["invalid"]) else 0

    result = pull_cdc_wonder(
        args.dataset, codes=args.codes, tree_path=args.tree_path, cause_axis=args.cause_axis,
        allow_all_causes=args.allow_all_causes, year_range=year_range,
        geography_levels=args.levels, age_grouping=args.age,
        stratify_sex=not args.no_sex, stratify_race=not args.no_race,
        extra_queries=extra, output_dir=args.output_dir, states=args.states, only=args.only,
        config_path=args.config, headless=not args.no_headless,
    )
    return 1 if result["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
