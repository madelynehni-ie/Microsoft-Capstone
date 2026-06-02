from __future__ import annotations
import argparse
import csv
import io
import json
import logging
import tarfile
import urllib.request
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import xml.etree.ElementTree as ET

# Silver layer schemas
NOTICE_FIELDS = [
    "notice_id", "publication_date", "notice_type", "country_codes",
    "buyer_name", "winner_name", "total_award_amount", "award_currency",
]
ORGANIZATION_FIELDS = ["notice_id", "org_id", "name", "role", "country", "town"]
LOT_FIELDS = ["notice_id", "lot_id", "title", "lot_amount", "lot_currency"]
CPV_FIELDS = ["notice_id", "cpv_code", "cpv_division"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TED Procurement Data ETL Pipeline (eForms)")
    parser.add_argument("--packages", "-p", nargs="+", default=[],
        help="Package IDs (e.g. 2026-04) or ranges (e.g. 2026-01..2026-04)")
    parser.add_argument("--local-archive", "-l", nargs="+", default=[],
        help="Local archive paths, optionally prefixed with package_id: (e.g. 2026-04:/path/to/file.tar.gz)")
    parser.add_argument("--data-dir", "-d", type=Path, default=Path("./data"),
        help="Base directory for outputs (default: ./data)")
    parser.add_argument("--base-url", "-u", default="https://ted.europa.eu/",
        help="Base URL for TED downloads (default: https://ted.europa.eu/)")
    parser.add_argument("--max-files", "-n", type=int, default=None,
        help="Max XML files per package — for quick testing")
    parser.add_argument("--no-bronze-xml", action="store_true",
        help="Skip writing raw XML to bronze layer (saves ~97%% disk space for bulk runs)")
    parser.add_argument("--overwrite", action="store_true",
        help="Re-download packages even if already present locally")
    parser.add_argument("--country", "-c", nargs="+", default=[],
        help="Filter by 3-letter country code(s) e.g. FRA DEU ESP")
    parser.add_argument("--cpv-prefix", default=None,
        help="Filter by CPV prefix e.g. 45 for construction")
    parser.add_argument("--notice-type", nargs="+", default=[],
        help="Filter by notice type string e.g. cn-standard-17 can-standard-29")
    parser.add_argument("--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    return parser.parse_args()


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def expand_package_ids(packages: list[str]) -> list[str]:
    expanded = []
    for pkg in packages:
        if ".." in pkg:
            try:
                start, end = pkg.split("..")
                sy, sm = map(int, start.split("-"))
                ey, em = map(int, end.split("-"))
                cy, cm = sy, sm
                while (cy, cm) <= (ey, em):
                    expanded.append(f"{cy}-{cm:02d}")
                    cm += 1
                    if cm > 12:
                        cm = 1
                        cy += 1
            except Exception as e:
                logging.error(f"Failed to expand range '{pkg}': {e}")
        else:
            expanded.append(pkg)
    return expanded


def parse_local_archives(args: list[str]) -> dict[str, Path]:
    result = {}
    for arg in args:
        if ":" in arg:
            pkg_id, path_str = arg.split(":", 1)
            result[pkg_id] = Path(path_str)
        else:
            path = Path(arg)
            result[path.stem.split(".")[0]] = path
    return result


def download_package(package_id: str, base_url: str, raw_dir: Path, overwrite: bool) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    archive_path = raw_dir / f"{package_id}.tar.gz"
    if archive_path.exists() and not overwrite:
        logging.info(f"Package {package_id} already cached at {archive_path}")
        return archive_path
    # TED URL: /packages/monthly/YYYY-M  (month not zero-padded, no file extension)
    year, month = package_id.split("-")
    url = f"{base_url.rstrip('/')}/packages/monthly/{year}-{int(month)}"
    logging.info(f"Downloading {package_id} from {url}")
    try:
        urllib.request.urlretrieve(url, archive_path)
        logging.info(f"Downloaded {archive_path.stat().st_size / 1_000_000:.1f} MB")
    except Exception as e:
        logging.warning(f"Download failed ({e}), falling back to mock package")
        _write_mock_package(archive_path, package_id)
    return archive_path


def _write_mock_package(archive_path: Path, package_id: str) -> None:
    """Creates a nested monthly→daily→XML mock that matches the real TED eForms structure."""
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    inner_buf = io.BytesIO()
    with tarfile.open(fileobj=inner_buf, mode="w:gz") as inner:
        for i in range(1, 3):
            xml = (
                f'<?xml version="1.0" encoding="UTF-8"?>\n'
                f'<ContractNotice xmlns="urn:oasis:names:specification:ubl:schema:xsd:ContractNotice-2"\n'
                f'  xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2"\n'
                f'  xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2"\n'
                f'  xmlns:efac="http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1"\n'
                f'  xmlns:efext="http://data.europa.eu/p27/eforms-ubl-extensions/1"\n'
                f'  xmlns:ext="urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2">\n'
                f'  <ext:UBLExtensions><ext:UBLExtension><ext:ExtensionContent>\n'
                f'    <efext:EformsExtension>\n'
                f'      <efac:NoticeSubType><cbc:SubTypeCode listName="notice-subtype">17</cbc:SubTypeCode></efac:NoticeSubType>\n'
                f'      <efac:Organizations>\n'
                f'        <efac:Organization><efac:Company>\n'
                f'          <cac:PartyIdentification><cbc:ID schemeName="organization">ORG-0001</cbc:ID></cac:PartyIdentification>\n'
                f'          <cac:PartyName><cbc:Name languageID="FRA">Mock Agency {i}</cbc:Name></cac:PartyName>\n'
                f'          <cac:PostalAddress>\n'
                f'            <cbc:CityName>Paris</cbc:CityName>\n'
                f'            <cac:Country><cbc:IdentificationCode listName="country">FRA</cbc:IdentificationCode></cac:Country>\n'
                f'          </cac:PostalAddress>\n'
                f'        </efac:Company></efac:Organization>\n'
                f'      </efac:Organizations>\n'
                f'    </efext:EformsExtension>\n'
                f'  </ext:ExtensionContent></ext:UBLExtension></ext:UBLExtensions>\n'
                f'  <cbc:ContractFolderID>mock-{package_id}-{i:04d}</cbc:ContractFolderID>\n'
                f'  <cbc:IssueDate>{package_id[:4]}-{package_id[5:7]}-0{i}</cbc:IssueDate>\n'
                f'  <cbc:NoticeTypeCode listName="competition">cn-standard</cbc:NoticeTypeCode>\n'
                f'  <cac:ContractingParty><cac:Party>\n'
                f'    <cac:PartyIdentification><cbc:ID schemeName="organization">ORG-0001</cbc:ID></cac:PartyIdentification>\n'
                f'  </cac:Party></cac:ContractingParty>\n'
                f'  <cac:ProcurementProject>\n'
                f'    <cbc:Name>Mock Construction Project {i}</cbc:Name>\n'
                f'    <cac:RequestedTenderTotal>\n'
                f'      <cbc:EstimatedOverallContractAmount currencyID="EUR">{100000 * i}</cbc:EstimatedOverallContractAmount>\n'
                f'    </cac:RequestedTenderTotal>\n'
                f'    <cac:MainCommodityClassification>\n'
                f'      <cbc:ItemClassificationCode listName="cpv">45210000</cbc:ItemClassificationCode>\n'
                f'    </cac:MainCommodityClassification>\n'
                f'  </cac:ProcurementProject>\n'
                f'</ContractNotice>\n'
            ).encode("utf-8")
            info = tarfile.TarInfo(name=f"mock_day/mock_{i:06d}.xml")
            info.size = len(xml)
            inner.addfile(info, io.BytesIO(xml))
    inner_bytes = inner_buf.getvalue()
    with tarfile.open(archive_path, "w:gz") as outer:
        info = tarfile.TarInfo(name=f"mock/{package_id}_mock.tar.gz")
        info.size = len(inner_bytes)
        outer.addfile(info, io.BytesIO(inner_bytes))
    logging.info(f"Mock package written to {archive_path}")


# ---------------------------------------------------------------------------
# eForms XML parser
# ---------------------------------------------------------------------------

def _strip_ns(root: ET.Element) -> None:
    for el in root.iter():
        if "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]


def _text(el: ET.Element | None) -> str:
    return el.text.strip() if el is not None and el.text else ""

def _find_org_id(parent: ET.Element) -> str:
    org_id = _text(parent.find(".//ID[@schemeName='organization']"))
    if org_id:
        return org_id
    return _text(parent.find(".//ID"))


def _find_country(company: ET.Element) -> str:
    country = _text(company.find(".//IdentificationCode[@listName='country']"))
    if country:
        return country.upper()
    return _text(company.find(".//Country//IdentificationCode")).upper()

def parse_eforms_xml(
    filename: str,
    xml_content: bytes,
    package_id: str,
    bronze_dir: Path,
    keep_bronze_xml: bool,
    filters: dict[str, Any],
) -> dict[str, Any]:
    try:
        if keep_bronze_xml:
            safe = filename.replace("/", "_").replace("\\", "_")
            (bronze_dir / safe).write_bytes(xml_content)

        root = ET.fromstring(xml_content)
        _strip_ns(root)

        # notice_id from filename stem (last path component, no extension)
        notice_id = filename.split("/")[-1].split(".")[0]

        # Notice type: "cn-standard-17", "can-standard-29", etc.
        notice_type = _text(root.find(".//NoticeTypeCode")) or "UNKNOWN"
        subtype = _text(root.find(".//SubTypeCode"))
        if subtype:
            notice_type = f"{notice_type}-{subtype}"

        # Early filter — skip before heavier parsing
        if filters.get("notice_type") and notice_type not in filters["notice_type"]:
            return {"skipped": True}

        # Publication date: "2026-04-27+02:00" → "2026-04-27"
        raw_date = _text(root.find(".//IssueDate"))
        pub_date = raw_date[:10] if raw_date else ""

        # Build org lookup: ORG-XXXX → {name, city, country}
        # All organizations are declared once in <efac:Organizations> at the top.
        org_lookup: dict[str, dict[str, str]] = {}
        for org in root.findall(".//Organization"):
            company = org.find("Company")
            if company is None:
                continue
            # ID with schemeName="organization" is the canonical org ref
            org_id = _find_org_id(company)
            if not org_id:
                continue
            org_lookup[org_id] = {
                "name": _text(company.find(".//Name")),
                "city": _text(company.find(".//CityName")),
                "country": _find_country(company),
            }

        # Buyers: ContractingParty references an org by ID
        buyer_org_ids: list[str] = []
        for cp in root.findall(".//ContractingParty"):
            oid = _find_org_id(wp)
            if oid:
                buyer_org_ids.append(oid)

        countries: set[str] = set()
        buyer_names: list[str] = []
        for oid in buyer_org_ids:
            org = org_lookup.get(oid, {})
            if org.get("name"):
                buyer_names.append(org["name"])
            if org.get("country"):
                countries.add(org["country"])

        country_codes = "|".join(sorted(countries))
        primary_buyer = buyer_names[0] if buyer_names else "UNKNOWN"

        # Country filter
        if filters.get("country") and not (countries & filters["country"]):
            return {"skipped": True}

        # CPV codes — filter by listName="cpv" to avoid other classification codes
        cpv_codes: list[str] = []
        cpv_rows_extracted: list[dict[str, str]] = []
        for el in root.findall(".//ItemClassificationCode"):
            if el.get("listName") == "cpv" and el.text:
                code = el.text.strip()
                cpv_codes.append(code)
                cpv_rows_extracted.append({
                    "notice_id": notice_id,
                    "cpv_code": code,
                    "cpv_division": code[:2] if len(code) >= 2 else "UNKNOWN",
                })

        if filters.get("cpv_prefix") and cpv_codes:
            if not any(c.startswith(filters["cpv_prefix"]) for c in cpv_codes):
                return {"skipped": True}

        # Winners: WinningParty elements appear in ContractAwardNotice only
        winner_org_ids: list[str] = []
        for wp in root.findall(".//WinningParty"):
            oid = _text(wp.find(".//ID"))
            if oid:
                winner_org_ids.append(oid)
        winners = [org_lookup[oid] for oid in winner_org_ids if oid in org_lookup]
        primary_winner = winners[0]["name"] if winners else "UNKNOWN"

        # Award amount:
        #   PayableAmount   → actual settled amount (award notices)
        #   EstimatedOverallContractAmount → pre-award estimate (contract notices)
        _payable = root.find(".//PayableAmount")
        amount_el = _payable if _payable is not None else root.find(".//EstimatedOverallContractAmount")
        raw_amount = _text(amount_el)
        currency = amount_el.get("currencyID", "") if amount_el is not None else ""
        clean_amount = 0.0
        if raw_amount:
            try:
                clean_amount = float(raw_amount)
            except ValueError:
                pass

        notice = {
            "notice_id": notice_id,
            "publication_date": pub_date,
            "notice_type": notice_type,
            "country_codes": country_codes,
            "buyer_name": primary_buyer,
            "winner_name": primary_winner,
            "total_award_amount": clean_amount if clean_amount > 0 else "",
            "award_currency": currency.upper() if currency else "",
        }

        # Organizations table — buyers and winners with full address from org_lookup
        orgs_extracted: list[dict[str, str]] = []
        for oid in buyer_org_ids:
            org = org_lookup.get(oid, {})
            orgs_extracted.append({
                "notice_id": notice_id,
                "org_id": oid,
                "name": org.get("name", "UNKNOWN"),
                "role": "BUYER",
                "country": org.get("country", "UNKNOWN"),
                "town": org.get("city", "UNKNOWN"),
            })
        for oid, org in zip(winner_org_ids, winners):
            orgs_extracted.append({
                "notice_id": notice_id,
                "org_id": oid,
                "name": org.get("name", "UNKNOWN"),
                "role": "WINNER",
                "country": org.get("country", "UNKNOWN"),
                "town": org.get("city", "UNKNOWN"),
            })

        # Lots table — ProcurementProjectLot with per-lot amount when available
        lots_extracted: list[dict[str, Any]] = []
        for lot in root.findall(".//ProcurementProjectLot"):
            lot_id = _text(lot.find("ID")) or f"LOT_{len(lots_extracted) + 1}"
            title = _text(lot.find(".//Name"))
            amt_el = lot.find(".//EstimatedOverallContractAmount")
            lot_amount = _text(amt_el)
            lot_currency = amt_el.get("currencyID", "") if amt_el is not None else ""
            lots_extracted.append({
                "notice_id": notice_id,
                "lot_id": lot_id,
                "title": title,
                "lot_amount": lot_amount,
                "lot_currency": lot_currency.upper() if lot_currency else "",
            })

        return {
            "notice": notice,
            "organizations": orgs_extracted,
            "lots": lots_extracted,
            "cpv_rows": cpv_rows_extracted,
        }
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Archive processing — handles nested monthly→daily→XML layout
# ---------------------------------------------------------------------------

def process_archive(
    package_id: str,
    archive_path: Path,
    data_dir: Path,
    max_files: int | None,
    keep_bronze_xml: bool,
    filters: dict[str, Any],
) -> dict[str, Any]:
    logging.info(f"Processing {archive_path}")
    notices: list[dict] = []
    organizations: list[dict] = []
    lots: list[dict] = []
    cpv_rows: list[dict] = []
    errors: list[dict] = []
    parsed_files = 0
    skipped_by_filters = 0

    bronze_dir = data_dir / "bronze" / "packages" / package_id
    if keep_bronze_xml:
        bronze_dir.mkdir(parents=True, exist_ok=True)

    def handle_xml(filename: str, xml_content: bytes) -> None:
        nonlocal parsed_files, skipped_by_filters
        res = parse_eforms_xml(filename, xml_content, package_id, bronze_dir, keep_bronze_xml, filters)
        if "error" in res:
            errors.append({"file": filename, "error": res["error"]})
        elif res.get("skipped"):
            skipped_by_filters += 1
        else:
            notices.append(res["notice"])
            organizations.extend(res["organizations"])
            lots.extend(res["lots"])
            cpv_rows.extend(res["cpv_rows"])
            parsed_files += 1

    def done() -> bool:
        return max_files is not None and (parsed_files + skipped_by_filters) >= max_files

    def iter_tar(tar: tarfile.TarFile) -> None:
        """Recursively handle flat (XML) or nested (daily tar.gz inside monthly) layouts."""
        all_members = [m for m in tar.getmembers() if m.isfile()]
        xml_members = [m for m in all_members if m.name.endswith(".xml")]
        nested_tars = [m for m in all_members if m.name.endswith(".tar.gz")]

        if xml_members:
            for member in xml_members:
                if done():
                    break
                f = tar.extractfile(member)
                if f:
                    handle_xml(member.name, f.read())
        elif nested_tars:
            # Monthly archive: iterate through each daily archive
            for daily_member in nested_tars:
                if done():
                    break
                f = tar.extractfile(daily_member)
                if not f:
                    continue
                try:
                    with tarfile.open(fileobj=f, mode="r:gz") as daily_tar:
                        iter_tar(daily_tar)
                except Exception as e:
                    errors.append({"file": daily_member.name, "error": str(e)})

    try:
        if tarfile.is_tarfile(archive_path):
            with tarfile.open(archive_path, "r:gz") as tar:
                iter_tar(tar)
        elif zipfile.is_zipfile(archive_path):
            with zipfile.ZipFile(archive_path, "r") as z:
                names = [n for n in z.namelist() if n.endswith(".xml")]
                for name in names:
                    if done():
                        break
                    handle_xml(name, z.read(name))
        else:
            errors.append({"file": str(archive_path), "error": "Unsupported archive type"})
    except Exception as e:
        logging.error(f"Error reading {archive_path}: {e}")
        errors.append({"file": str(archive_path), "error": str(e)})

    logging.info(
        f"  {package_id}: {parsed_files} parsed, "
        f"{skipped_by_filters} filtered, {len(errors)} errors"
    )
    return {
        "notices": notices,
        "organizations": organizations,
        "lots": lots,
        "cpv_rows": cpv_rows,
        "errors": errors,
        "parsed_files": parsed_files,
        "skipped_by_filters": skipped_by_filters,
    }


# ---------------------------------------------------------------------------
# CSV writing — append mode for incremental multi-package runs
# ---------------------------------------------------------------------------

def write_csv(
    path: Path,
    rows: list[dict[str, Any]],
    fields: list[str],
    append: bool = False,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append else "w"
    write_header = not append or not path.exists() or path.stat().st_size == 0
    with open(path, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
    action = "Appended" if append and not write_header else "Wrote"
    logging.info(f"  {action} {len(rows)} rows → {path}")


# ---------------------------------------------------------------------------
# Gold aggregations and quality profile
# ---------------------------------------------------------------------------

def aggregate_by_date(notices: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for notice in notices:
        date = notice.get("publication_date")
        if not date:
            continue
        if date not in grouped:
            grouped[date] = {
                "publication_date": date,
                "notice_count": 0,
                "award_amount_eur": 0.0,
                "unique_buyers": set(),
                "unique_winners": set(),
            }
        row = grouped[date]
        row["notice_count"] += 1
        buyer = notice.get("buyer_name")
        if buyer and buyer != "UNKNOWN":
            row["unique_buyers"].add(buyer)
        winner = notice.get("winner_name")
        if winner and winner != "UNKNOWN":
            row["unique_winners"].add(winner)
        if notice.get("award_currency") == "EUR":
            val = notice.get("total_award_amount")
            if val not in (None, ""):
                row["award_amount_eur"] += float(val)

    result = []
    for row in grouped.values():
        result.append({
            **row,
            "unique_buyers": len(row["unique_buyers"]),
            "unique_winners": len(row["unique_winners"]),
        })
    return sorted(result, key=lambda r: r["publication_date"])


def build_gold_outputs(
    data_dir: Path,
    notices: list[dict[str, Any]],
    cpv_rows: list[dict[str, Any]],
) -> None:
    daily = aggregate_by_date(notices)
    write_csv(
        data_dir / "gold" / "daily_aggregates.csv", daily,
        ["publication_date", "notice_count", "award_amount_eur", "unique_buyers", "unique_winners"],
    )
    cpv_counts = Counter(r["cpv_division"] for r in cpv_rows if r.get("cpv_division"))
    write_csv(
        data_dir / "gold" / "cpv_aggregates.csv",
        [{"cpv_division": d, "count": c} for d, c in cpv_counts.most_common()],
        ["cpv_division", "count"],
    )


def build_profile(
    package_ids: list[str],
    notices: list[dict[str, Any]],
    org_count: int,
    lot_count: int,
    cpv_rows: list[dict[str, Any]],
    errors: list[dict[str, str]],
    parsed_files: int,
    skipped_by_filters: int,
) -> dict[str, Any]:
    def counter_field(rows: list[dict], field: str, limit: int = 20) -> dict[str, int]:
        return dict(Counter(str(r.get(field) or "UNKNOWN") for r in rows).most_common(limit))

    def counter_pipe_field(rows: list[dict], field: str, limit: int = 20) -> dict[str, int]:
        c: Counter[str] = Counter()
        for row in rows:
            vals = [v.strip() for v in str(row.get(field) or "").split("|") if v.strip()]
            c.update(vals or ["UNKNOWN"])
        return dict(c.most_common(limit))

    def missingness(rows: list[dict], fields: list[str]) -> dict[str, float]:
        if not rows:
            return {f: 1.0 for f in fields}
        return {
            f: round(sum(1 for r in rows if r.get(f) in (None, "")) / len(rows), 4)
            for f in fields
        }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "package_ids": package_ids,
        "counts": {
            "parsed_xml_files": parsed_files,
            "notices": len(notices),
            "organizations": org_count,
            "lots": lot_count,
            "cpv_rows": len(cpv_rows),
            "skipped_by_filters": skipped_by_filters,
            "parse_errors": len(errors),
        },
        "top_notice_types": counter_field(notices, "notice_type"),
        "top_countries": counter_pipe_field(notices, "country_codes"),
        "top_cpv_divisions": counter_field(cpv_rows, "cpv_division"),
        "notice_missingness": missingness(notices, NOTICE_FIELDS),
        "parse_errors": errors[:50],
    }


# ---------------------------------------------------------------------------
# Main — incremental per-package writes, bounded memory
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)

    package_ids = expand_package_ids(args.packages)
    local_archives = parse_local_archives(args.local_archive)
    data_dir = args.data_dir
    raw_dir = data_dir / "raw" / "packages"
    filters = {
        "country": {v.upper() for v in args.country} if args.country else None,
        "cpv_prefix": args.cpv_prefix,
        "notice_type": set(args.notice_type) if args.notice_type else None,
    }

    # Gold aggregation needs notices + cpv_rows across all packages (manageable in RAM).
    # Silver CSVs are written incrementally — only one package in memory at a time.
    all_notices_gold: list[dict[str, Any]] = []
    all_cpv_rows_gold: list[dict[str, Any]] = []
    all_errors: list[dict[str, str]] = []
    total_parsed = total_skipped = total_orgs = total_lots = 0
    first_write = True

    for package_id in package_ids:
        archive_path = local_archives.get(package_id) or download_package(
            package_id, args.base_url, raw_dir, args.overwrite
        )
        result = process_archive(
            package_id=package_id,
            archive_path=archive_path,
            data_dir=data_dir,
            max_files=args.max_files,
            keep_bronze_xml=not args.no_bronze_xml,
            filters=filters,
        )

        # Write Silver CSVs — header only on first package, append thereafter
        append = not first_write
        silver = data_dir / "silver"
        write_csv(silver / "notices.csv",       result["notices"],       NOTICE_FIELDS,       append)
        write_csv(silver / "organizations.csv", result["organizations"], ORGANIZATION_FIELDS, append)
        write_csv(silver / "lots.csv",          result["lots"],          LOT_FIELDS,          append)
        write_csv(silver / "cpv_codes.csv",     result["cpv_rows"],      CPV_FIELDS,          append)
        first_write = False

        # Accumulate only what gold aggregation needs
        all_notices_gold.extend(result["notices"])
        all_cpv_rows_gold.extend(result["cpv_rows"])
        all_errors.extend(result["errors"])
        total_parsed  += result["parsed_files"]
        total_skipped += result["skipped_by_filters"]
        total_orgs    += len(result["organizations"])
        total_lots    += len(result["lots"])

    build_gold_outputs(data_dir, all_notices_gold, all_cpv_rows_gold)

    profile = build_profile(
        package_ids, all_notices_gold, total_orgs, total_lots,
        all_cpv_rows_gold, all_errors, total_parsed, total_skipped,
    )
    report_path = data_dir / "reports" / "etl_profile.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(profile, indent=2, ensure_ascii=False), encoding="utf-8")

    logging.info("Silver → %s", data_dir / "silver")
    logging.info("Gold   → %s", data_dir / "gold")
    logging.info("Report → %s", report_path)


if __name__ == "__main__":
    main()
