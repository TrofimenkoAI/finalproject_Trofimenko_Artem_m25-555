from valutatrade_hub.cli.interface import build_parser, execute


def main(argv=None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    msg = execute(args)
    print(msg)


if __name__ == "__main__":
    main()
