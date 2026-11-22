"""Settings from the service's environment file and process environment.

Secrets are wrapped in `Secret`, which never prints its value: not in repr, str, f-strings,
logging or tracebacks. Parse errors name the line number and key, never the value, since the
value may be a secret.

`docker compose --env-file .env` reads the same file to set the catalog's password, and compose
expands `$VAR`, drops inline ` # comments` and handles backslash escapes. Values using any of
those are refused, so both readers always see the same value.
"""
from __future__ import annotations

import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path

DEFAULT_ENV_FILE = ".env"
_COMPOSE_SPECIAL = re.compile(r"\$|\\|\s#")


class ConfigError(Exception):
    pass


class Secret:
    """A value that must never be shown. Call `reveal()` at the one place it is used."""

    __slots__ = ("_value",)

    def __init__(self, value: str):
        self._value = value

    def reveal(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "Secret('<redacted>')"

    __str__ = __repr__

    def __format__(self, spec: str) -> str:
        return repr(self)

    def __reduce__(self):
        raise TypeError("Secret can't be pickled")

    def __eq__(self, other) -> bool:
        return isinstance(other, Secret) and other._value == self._value

    def __hash__(self) -> int:
        return hash(self._value)


def parse_env_file(path: Path) -> dict[str, str]:
    """KEY=VALUE lines; blank lines, `#` comments and an `export ` prefix allowed; values may be quoted."""
    out: dict[str, str] = {}
    for n, raw in enumerate(path.read_text().splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, sep, value = line.partition("=")
        key = key.strip()
        if not sep or not key.isidentifier():
            raise ConfigError(f"{path}: line {n} isn't KEY=VALUE")
        value = value.strip()
        if _COMPOSE_SPECIAL.search(value):
            raise ConfigError(f"{path}: line {n} ({key}) has `$`, a backslash or ` #`, which docker compose "
                              "reads differently; use a value without them")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        out[key] = value
    return out


def check_private(path: Path) -> None:
    """The file holds secrets, so only its owner may have any access (README: Deployment)."""
    mode = path.stat().st_mode
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise ConfigError(f"{path} is accessible by others (mode {stat.S_IMODE(mode):o}); chmod 600 it")


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    catalog_host: str
    catalog_port: int
    catalog_user: str
    catalog_db: str
    catalog_password: Secret
    host: str
    port: int
    thetadata_api_key: Secret | None


def load_settings(env_file: str | os.PathLike | None = None, environ: dict[str, str] | None = None) -> Settings:
    """Read the env file (MD_ENV_FILE, else `.env`), then let the process environment override it.

    Only the default `.env` may be missing (under systemd the values arrive through
    EnvironmentFile); a file named explicitly must exist. Empty environment values don't
    override. A relative MD_DATA_DIR is taken from the env file's directory, not the working
    directory, and must be absolute when there's no env file.
    """
    environ = dict(os.environ if environ is None else environ)
    named = env_file or environ.get("MD_ENV_FILE")
    path = Path(named or DEFAULT_ENV_FILE)
    values: dict[str, str] = {}
    base: Path | None = None
    if path.exists():
        check_private(path)
        values = parse_env_file(path)
        base = path.resolve().parent
    elif named:
        raise ConfigError(f"env file {path} doesn't exist")
    values.update({k: v for k, v in environ.items()
                   if v and (k.startswith("MD_") or k == "THETADATA_API_KEY")})

    def get(key: str, default: str | None = None) -> str:
        v = values.get(key) or default
        if v is None:
            raise ConfigError(f"{key} isn't set (env file {path} or the environment)")
        return v

    def get_int(key: str, default: str) -> int:
        v = get(key, default)
        try:
            return int(v)
        except ValueError:
            raise ConfigError(f"{key} must be an integer") from None

    data_dir = Path(get("MD_DATA_DIR", "data"))
    if not data_dir.is_absolute():
        if base is None:
            raise ConfigError("MD_DATA_DIR must be absolute when there's no env file")
        data_dir = base / data_dir

    key = values.get("THETADATA_API_KEY")
    return Settings(
        data_dir=data_dir.resolve(),
        catalog_host=get("MD_CATALOG_HOST", "127.0.0.1"),
        catalog_port=get_int("MD_CATALOG_PORT", "55433"),
        catalog_user=get("MD_CATALOG_USER", "market_data"),
        catalog_db=get("MD_CATALOG_DB", "md_catalog"),
        catalog_password=Secret(get("MD_CATALOG_PASSWORD")),
        host=get("MD_HOST", "127.0.0.1"),
        port=get_int("MD_PORT", "8780"),
        thetadata_api_key=Secret(key) if key else None,
    )
