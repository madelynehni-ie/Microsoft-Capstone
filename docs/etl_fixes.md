# ETL Pipeline Fixes & Upgrades

This document explains the changes made to `Exploratory ETL/ted_exploratory_etl.py` after running it against real TED data for the first time. The original script was built and tested against mock data; running it on a real monthly package revealed three structural mismatches that required fixes.

---

## What Was Wrong

### 1. Wrong download URL

**Original:**
```python
url = f"{base_url}/packages/{package_id}.tar.gz"
# → https://ted.europa.eu/packages/2026-04.tar.gz  (404)
```

**Fix:**
```python
year, month = package_id.split("-")
url = f"{base_url}/packages/monthly/{year}-{int(month)}"
# → https://ted.europa.eu/packages/monthly/2026-4   (200, ~400 MB)
```

Two things changed: the path is `/packages/monthly/`, the month is not zero-padded (`4` not `04`), and there is no `.tar.gz` extension in the URL (the server returns gzip content directly).

---

### 2. Nested archive structure

The monthly `.tar.gz` does not contain XML files directly. It contains ~21 daily `.tar.gz` archives, each holding ~4,000 XML files:

```
2026-04.tar.gz
  └── 04/
        ├── 20260401_2026064.tar.gz   (~4,000 XML notices)
        ├── 20260402_2026065.tar.gz
        ...
        └── 20260429_2026083.tar.gz
```

The original `process_archive` only looked for `.xml` members at the top level of the archive. When it found none (only `.tar.gz` daily archives), it processed zero files and silently produced empty outputs.

**Fix:** `process_archive` now uses a recursive `iter_tar` function that detects whether the current archive contains XML files directly (flat layout) or nested `.tar.gz` files (monthly layout), and handles both:

```python
def iter_tar(tar):
    xml_members = [m for m in tar.getmembers() if m.name.endswith(".xml")]
    nested_tars = [m for m in tar.getmembers() if m.name.endswith(".tar.gz")]

    if xml_members:
        # flat layout — parse directly
    elif nested_tars:
        # nested layout — recurse into each daily archive
        for daily in nested_tars:
            with tarfile.open(fileobj=tar.extractfile(daily), mode="r:gz") as daily_tar:
                iter_tar(daily_tar)
```

This handles both the real monthly layout and any flat archive (e.g., a single daily package passed via `--local-archive`).

---

### 3. Wrong XML schema — TED migrated to eForms in 2024

This was the most significant issue. The original parser was written for the **TED R2.0.9 format** (used up to ~2023). All data from 2024 onwards uses the **eForms UBL format**, which has a completely different structure.

| Field | R2.0.9 (old) | eForms (current) |
|---|---|---|
| Root element | `<TED_EXPORT>` | `<ContractNotice>` / `<ContractAwardNotice>` |
| Publication date | `<DATE_PUB>20260427</DATE_PUB>` | `<cbc:IssueDate>2026-04-27+02:00</cbc:IssueDate>` |
| Notice type | Tag name of `<FORM_SECTION>` child (e.g. `F02_2014`) | `<cbc:NoticeTypeCode>cn-standard</cbc:NoticeTypeCode>` + `<cbc:SubTypeCode>16</cbc:SubTypeCode>` |
| Country | `<ISO_COUNTRY VALUE="FR"/>` | `<cbc:IdentificationCode listName="country">FRA</cbc:IdentificationCode>` (3-letter ISO) |
| Organizations | Inline inside `<CONTRACTING_BODY>` and `<WINNER>` | All declared once in `<efac:Organizations>` block with `ORG-XXXX` IDs, then referenced by ID |
| Buyer name | `<ADDRESS_CONTRACTING_BODY><OFFICIALNAME>` | `<cac:ContractingParty>` → references org ID → look up in org block |
| Winner name | `<WINNER><ADDRESS_WINNER><OFFICIALNAME>` | `<cac:WinningParty>` → references org ID → look up in org block |
| CPV code | `<CPV_CODE CODE="45210000-2">` (attribute, with suffix) | `<cbc:ItemClassificationCode listName="cpv">45210000</cbc:ItemClassificationCode>` (text, no suffix) |
| Award amount | `<VAL_TOTAL CURRENCY="EUR">` | `<cbc:PayableAmount currencyID="EUR">` (settled) or `<cbc:EstimatedOverallContractAmount currencyID="EUR">` (pre-award) |
| Lots | `<LOT>` / `<LOT_DIVISION>` | `<cac:ProcurementProjectLot>` |
| Buyer town | Not extracted (ETL gap) | `<cbc:CityName>` inside org block — **now extracted** |
| Winner town/country | Not extracted (ETL gap) | Same org block — **now extracted** |
| Lot amount | Hardcoded empty (ETL gap) | `<cbc:EstimatedOverallContractAmount>` inside each lot — **now extracted** |

**Fix:** `parse_single_xml` was replaced entirely with `parse_eforms_xml`, which:

1. Strips all XML namespaces from tags (same technique as before, works fine with eForms)
2. Builds an in-memory org lookup dict from the `<efac:Organizations>` block: `ORG-XXXX → {name, city, country}`
3. Resolves buyers via `<cac:ContractingParty>` → org ID → lookup
4. Resolves winners via `<cac:WinningParty>` → org ID → lookup
5. Parses `IssueDate` and strips the timezone offset (`2026-04-27+02:00` → `2026-04-27`)
6. Combines `NoticeTypeCode` + `SubTypeCode` into a single string (e.g. `cn-standard-16`)
7. Filters `ItemClassificationCode` by `listName="cpv"` to avoid other classification schemes
8. Tries `PayableAmount` first (actual settled amount in award notices), falls back to `EstimatedOverallContractAmount`

The three ETL gaps from the original (buyer town, winner town/country, lot amount) are now all populated correctly because eForms stores everything in the centralized org block.

---

## Memory fix for multi-month runs

The original `main()` accumulated all notices, organizations, lots, and CPV rows from every package into RAM before writing any CSV. For a single mock package this was fine, but for real monthly runs (~77K notices, ~257K lots, ~590K CPV rows) and especially for multi-month date ranges, this would have grown unbounded.

**Fix:** Silver CSVs are now written **incrementally per package** using append mode. Only the data needed for Gold aggregation (notices + CPV rows) is kept in memory across packages:

```python
for i, package_id in enumerate(package_ids):
    result = process_archive(...)

    # Write Silver immediately — append after the first package
    append = i > 0
    write_csv(silver / "notices.csv",       result["notices"],       ..., append)
    write_csv(silver / "organizations.csv", result["organizations"], ..., append)
    write_csv(silver / "lots.csv",          result["lots"],          ..., append)
    write_csv(silver / "cpv_codes.csv",     result["cpv_rows"],      ..., append)

    # Only keep what Gold aggregation needs
    all_notices_gold.extend(result["notices"])
    all_cpv_rows_gold.extend(result["cpv_rows"])
    # result goes out of scope → memory freed
```

`write_csv` now accepts an `append` flag and writes the CSV header only on the first write of a run.

---

## Mock package updated

The fallback mock package (generated when the download fails) was also updated to match the eForms structure and the nested monthly→daily→XML layout, so the mock still exercises exactly the same code paths as real data.

---

## Real data baseline (April 2026)

After all fixes, a full run on `2026-04` (400 MB compressed) completed in **~3.5 minutes** with:

| | |
|---|---|
| XML files parsed | 77,691 |
| Parse errors | 0 |
| notices.csv rows | 77,691 |
| organizations.csv rows | 107,799 |
| lots.csv rows | 257,224 |
| cpv_codes.csv rows | 589,875 |
| `total_award_amount` missingness | 35% (expected — pre-award notices have no amount) |

**Note on winners:** `winner_name` is `UNKNOWN` on pre-award contract notices (`cn-*` types), which is correct — no winner exists yet. To get labeled award data for ML, filter on award notice types: `can-standard-29`, `can-standard-30`, `can-standard-31`. These account for ~30,000 notices per month.
