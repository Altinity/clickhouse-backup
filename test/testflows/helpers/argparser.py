def argparser(parser):
    """Default argument parser for regressions.
    """
    parser.add_argument("--local",
                        action="store_true",
                        help="run regression in local mode without docker-compose down", default=True)
