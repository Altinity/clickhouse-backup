def argparser(parser):
    """Default argument parser for regressions.
    """
    parser.add_argument("--local",
                        action="store_true",
                        help="run regression in local mode without container cleanup", default=True)
    parser.add_argument("--stress",
                        action="store_true",
                        default=False,
                        help=(
                            "enable exhaustive cipher / TLS-suite coverage in FIPS scenarios; "
                            "without this flag the FIPS suite checks the documented minimum "
                        ))
                        
    parser.add_argument("--fips",
                        action="store_true",
                        default=False,
                        help=(
                            "tag the run as FIPS-strict (`self.context.fips_strict=True`); "
                            "future work will re-run the broad regression against the FIPS "
                            "backup container with GODEBUG=fips140=only at container level"
                        ))
    parser.add_argument("--fips-godebug",
                        type=str,
                        choices=["unset", "on", "only", "off"],
                        default="only",
                        help=(
                            "Select the GODEBUG fips140 mode for the FIPS suite "
                            "(exported on the `clickhouse_backup_fips` container): "
                            "'unset' (do not set GODEBUG; use build-time default), "
                            "'on' (FIPS enabled, non-strict), "
                            "'only' (FIPS enabled, strict; default), "
                            "or 'off' (disable FIPS at runtime)."
                        ))
