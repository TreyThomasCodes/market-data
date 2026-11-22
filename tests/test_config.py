import logging
import pickle

import pytest

from market_data.config import ConfigError, Secret, load_settings, parse_env_file

PW = "hunter2-s3cret-VALUE"


def write_env(tmp_path, text, mode=0o600):
    p = tmp_path / "env"
    p.write_text(text)
    p.chmod(mode)
    return p


def test_parse_forms(tmp_path):
    p = write_env(tmp_path, "# c\n\nA=1\nexport B = two \nC='q v'\nD=\"x=y\"\nE=\n")
    assert parse_env_file(p) == {"A": "1", "B": "two", "C": "q v", "D": "x=y", "E": ""}


def test_secret_never_shows(caplog):
    s = Secret(PW)
    shown = [repr(s), str(s), f"{s}", f"{s!r}", "%s" % s, repr([s]), repr({"k": s})]
    assert not any(PW in x for x in shown)
    with caplog.at_level(logging.DEBUG):
        logging.getLogger("t").info("value %s %r", s, s)
    assert PW not in caplog.text
    with pytest.raises(TypeError):
        pickle.dumps(s)
    assert s.reveal() == PW


def test_settings_repr_hides_secrets(tmp_path):
    p = write_env(tmp_path, f"MD_CATALOG_PASSWORD={PW}\nTHETADATA_API_KEY={PW}x\n")
    s = load_settings(p, environ={})
    assert PW not in repr(s)
    assert s.catalog_password.reveal() == PW
    assert s.thetadata_api_key.reveal() == PW + "x"


def test_bad_line_error_omits_the_line(tmp_path):
    p = write_env(tmp_path, f"OK=1\n{PW}\n")
    with pytest.raises(ConfigError) as e:
        load_settings(p, environ={})
    assert "line 2" in str(e.value) and PW not in str(e.value)


@pytest.mark.parametrize("mode", [0o644, 0o620, 0o601])
def test_any_access_by_others_is_refused(tmp_path, mode):
    p = write_env(tmp_path, f"MD_CATALOG_PASSWORD={PW}\n", mode=mode)
    with pytest.raises(ConfigError, match=f"accessible by others \\(mode {mode:o}\\)"):
        load_settings(p, environ={})


@pytest.mark.parametrize("value", ["ab$cd", "${X}", "a\\b", "8780 # api", "'q $x'"])
def test_values_compose_reads_differently_are_refused(tmp_path, value):
    p = write_env(tmp_path, f"MD_CATALOG_PASSWORD={value}\n")
    with pytest.raises(ConfigError, match="line 1 \\(MD_CATALOG_PASSWORD\\)") as e:
        load_settings(p, environ={})
    assert value not in str(e.value)


def test_hash_inside_a_value_is_fine(tmp_path):
    p = write_env(tmp_path, "MD_CATALOG_PASSWORD=a#b\n")
    assert load_settings(p, environ={}).catalog_password.reveal() == "a#b"


def test_named_env_file_must_exist(tmp_path):
    with pytest.raises(ConfigError, match="doesn't exist"):
        load_settings(tmp_path / "typo", environ={"MD_CATALOG_PASSWORD": "e"})
    with pytest.raises(ConfigError, match="doesn't exist"):
        load_settings(environ={"MD_ENV_FILE": str(tmp_path / "typo"), "MD_CATALOG_PASSWORD": "e"})


def test_empty_environment_value_doesnt_override(tmp_path):
    p = write_env(tmp_path, "MD_CATALOG_PASSWORD=file\nMD_PORT=9000\n")
    s = load_settings(p, environ={"MD_PORT": "", "MD_CATALOG_PASSWORD": ""})
    assert s.port == 9000 and s.catalog_password.reveal() == "file"


def test_relative_data_dir_follows_the_env_file(tmp_path, monkeypatch):
    p = write_env(tmp_path, "MD_CATALOG_PASSWORD=x\n")
    monkeypatch.chdir("/")
    assert load_settings(p, environ={}).data_dir == (tmp_path / "data").resolve()
    assert load_settings(p, environ={"MD_DATA_DIR": "lakes/a"}).data_dir == (tmp_path / "lakes/a").resolve()
    assert load_settings(p, environ={"MD_DATA_DIR": "/srv/x"}).data_dir == __import__("pathlib").Path("/srv/x")


def test_environment_overrides_file_and_defaults(tmp_path):
    p = write_env(tmp_path, "MD_CATALOG_PASSWORD=file\nMD_PORT=9000\n")
    s = load_settings(p, environ={"MD_PORT": "9001", "UNRELATED": "x"})
    assert s.port == 9001 and s.catalog_password.reveal() == "file"
    assert s.catalog_port == 55433 and s.host == "127.0.0.1" and s.thetadata_api_key is None


def test_missing_default_file_uses_environment(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # no .env here
    s = load_settings(environ={"MD_CATALOG_PASSWORD": "e", "MD_DATA_DIR": "/srv/md"})
    assert s.catalog_password.reveal() == "e"


def test_relative_data_dir_without_env_file_is_an_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ConfigError, match="MD_DATA_DIR must be absolute"):
        load_settings(environ={"MD_CATALOG_PASSWORD": "e"})


def test_missing_password_is_an_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ConfigError, match="MD_CATALOG_PASSWORD"):
        load_settings(environ={"MD_DATA_DIR": "/srv/md"})


def test_bad_int(tmp_path):
    p = write_env(tmp_path, "MD_CATALOG_PASSWORD=e\nMD_PORT=x\n")
    with pytest.raises(ConfigError, match="MD_PORT"):
        load_settings(p, environ={})
