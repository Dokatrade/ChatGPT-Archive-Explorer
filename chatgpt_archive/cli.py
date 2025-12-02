import argparse
from pathlib import Path
from typing import Any

from .importer import ImportOptions, import_archive
from .server import ArchiveServer
from .utils import DEFAULT_SOURCE_ID


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="chatgpt-archive", description="Local ChatGPT archive explorer")
    subparsers = parser.add_subparsers(dest="command", required=True)

    import_cmd = subparsers.add_parser("import", help="Import a ChatGPT export (folder or .zip)")
    import_cmd.add_argument("export_path", type=Path, help="Path to extracted export folder or .zip")
    import_cmd.add_argument(
        "--output",
        type=Path,
        required=True,
        dest="output_root",
        help="Target archive folder (will be created if missing)",
    )
    import_cmd.add_argument(
        "--account",
        "--source",
        dest="source_id",
        default=DEFAULT_SOURCE_ID,
        help="Source/account id label (used for grouping and avoiding collisions). Default: 'default'",
    )
    import_cmd.add_argument(
        "--allow-network-images",
        action="store_true",
        help="Permit downloading remote images (placeholder flag, off by default)",
    )
    import_cmd.add_argument(
        "--incremental",
        action="store_true",
        help="Append to existing archive without dropping previous imports (full rebuild by default)",
    )

    serve_cmd = subparsers.add_parser("serve", help="Serve local UI from an existing archive")
    serve_cmd.add_argument("--root", type=Path, required=True, help="Archive root folder (with projects/ and index.db)")
    serve_cmd.add_argument("--host", default="127.0.0.1", help="Listen host")
    serve_cmd.add_argument("--port", default=8000, type=int, help="Listen port")

    return parser.parse_args()


def run_import(args: argparse.Namespace) -> None:
    options = ImportOptions(
        export_path=args.export_path,
        output_root=args.output_root,
        allow_network_images=args.allow_network_images,
        incremental=args.incremental,
        source_id=args.source_id,
    )
    stats: Any = import_archive(options)
    print(
        f"Импорт завершен: бесед={stats['conversations']} (добавлено {stats['imported_conversations']}), проектов={stats['projects']}. "
        f"Архив: {stats['output_root']} | аккаунт: {stats['source_id']} | режим={'append' if stats['append_mode'] else 'rebuild'}"
    )
    print(f"Индекс: {stats['db_path']}")
    if stats.get("models"):
        print(f"Модели: {', '.join(stats['models'])}")


def run_server(args: argparse.Namespace) -> None:
    server = ArchiveServer(args.root, host=args.host, port=args.port)
    server.serve()


def main() -> None:
    args = _parse_args()
    if args.command == "import":
        run_import(args)
    elif args.command == "serve":
        run_server(args)
    else:
        raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
