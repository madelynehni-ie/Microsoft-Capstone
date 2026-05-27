        row["award_amount_eur"] += eur_amount(notice, "total_award_amount", "award_currency")

    rows: list[dict[str, Any]] = []
    for row in grouped.values():
        rows.append(
            {
                **row,
                "unique_buyers": len(row["unique_buyers"]),
                "unique_winners": len(row["unique_winners"]),
            }
        )
    return sorted(rows, key=lambda row: row["publication_date"])


def eur_amount(notice: dict[str, Any], amount_field: str, currency_field: str) -> float:
    if notice.get(currency_field) != "EUR":
        return 0.0
    value = notice.get(amount_field)
    return float(value) if value not in (None, "") else 0.0


def build_profile(
    package_ids: list[str],
    notices: list[dict[str, Any]],
    organizations: list[dict[str, Any]],
    lots: list[dict[str, Any]],
    cpv_rows: list[dict[str, Any]],
    errors: list[dict[str, str]],
    parsed_files: int,
    skipped_by_filters: int,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "package_ids": package_ids,
        "counts": {
            "parsed_xml_files": parsed_files,
            "notices": len(notices),
            "organizations": len(organizations),
            "lots": len(lots),
            "cpv_rows": len(cpv_rows),
            "skipped_by_filters": skipped_by_filters,
            "parse_errors": len(errors),
        },
        "top_notice_types": counter_from_field(notices, "notice_type"),
        "top_countries": counter_from_pipe_field(notices, "country_codes"),
        "top_cpv_divisions": counter_from_field(cpv_rows, "cpv_division"),
        "notice_missingness": missingness(notices, NOTICE_FIELDS),
        "parse_errors": errors[:50],
    }


def counter_from_field(rows: list[dict[str, Any]], field: str, limit: int = 20) -> dict[str, int]:
    counter = Counter(str(row.get(field) or "UNKNOWN") for row in rows)
    return dict(counter.most_common(limit))


def counter_from_pipe_field(rows: list[dict[str, Any]], field: str, limit: int = 20) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        values = split_pipe(row.get(field)) or ["UNKNOWN"]
        counter.update(values)
    return dict(counter.most_common(limit))


def missingness(rows: list[dict[str, Any]], fields: list[str]) -> dict[str, float]:
    if not rows:
        return {field: 1.0 for field in fields}
    return {
        field: round(sum(1 for row in rows if row.get(field) in (None, "")) / len(rows), 4)
        for field in fields
    }


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)

    package_ids = expand_package_ids(args.packages)
    local_archives = parse_local_archives(args.local_archive)
    data_dir = args.data_dir
    raw_dir = data_dir / "raw" / "packages"
    filters = {
        "country": {value.upper() for value in args.country} if args.country else None,
        "cpv_prefix": args.cpv_prefix,
        "notice_type": set(args.notice_type) if args.notice_type else None,
    }

    all_notices: list[dict[str, Any]] = []
    all_organizations: list[dict[str, Any]] = []
    all_lots: list[dict[str, Any]] = []
    all_cpv_rows: list[dict[str, Any]] = []
    all_errors: list[dict[str, str]] = []
    parsed_files = 0
    skipped_by_filters = 0

    for package_id in package_ids:
        archive_path = local_archives.get(package_id)
        if archive_path is None:
            archive_path = download_package(package_id, args.base_url, raw_dir, args.overwrite)

        result = process_archive(
            package_id=package_id,
            archive_path=archive_path,
            data_dir=data_dir,
            max_files=args.max_files,
            keep_bronze_xml=not args.no_bronze_xml,
            filters=filters,
        )
        all_notices.extend(result["notices"])
        all_organizations.extend(result["organizations"])
        all_lots.extend(result["lots"])
        all_cpv_rows.extend(result["cpv_rows"])
        all_errors.extend(result["errors"])
        parsed_files += result["parsed_files"]
        skipped_by_filters += result["skipped_by_filters"]

    write_csv(data_dir / "silver" / "notices.csv", all_notices, NOTICE_FIELDS)
    write_csv(data_dir / "silver" / "organizations.csv", all_organizations, ORGANIZATION_FIELDS)
    write_csv(data_dir / "silver" / "lots.csv", all_lots, LOT_FIELDS)
    write_csv(data_dir / "silver" / "cpv_codes.csv", all_cpv_rows, CPV_FIELDS)
    build_gold_outputs(data_dir, all_notices, all_cpv_rows)

    profile = build_profile(
        package_ids,
        all_notices,
        all_organizations,
        all_lots,
        all_cpv_rows,
        all_errors,
        parsed_files,
        skipped_by_filters,
    )
    report_path = data_dir / "reports" / "etl_profile.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(profile, indent=2, ensure_ascii=False), encoding="utf-8")

    logging.info("Wrote Silver outputs to %s", data_dir / "silver")
    logging.info("Wrote Gold outputs to %s", data_dir / "gold")
    logging.info("Wrote profile report to %s", report_path)


if __name__ == "__main__":
    main()
