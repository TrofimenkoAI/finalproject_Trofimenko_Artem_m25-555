import shlex
import sys

from valutatrade_hub.cli.interface import build_parser, execute
from valutatrade_hub.logging_config import configure_logging



def main(argv=None) -> None:
    configure_logging()
    parser = build_parser()


    if argv is None:
        argv = sys.argv[1:]

    if not argv:
        while True:
            try:
                line = input("> ")
            except EOFError:
                break
            if line is None:
                continue
            line = line.strip()
            if not line:
                continue
            if line.lower() in ("exit", "quit"):
                break
            if line.startswith("valutatrade "):
                line = line[len("valutatrade ") :].strip()
            try:
                args = parser.parse_args(shlex.split(line))
                msg = execute(args)
                print(msg)
            except SystemExit:
                pass
        return

    args = parser.parse_args(argv)
    msg = execute(args)
    print(msg)


if __name__ == "__main__":
    main()
