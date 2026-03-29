import sys

from kafka_collector.cli import run

import pytest


@pytest.mark.skipif(sys.version_info < (3, 13), reason="Python 3.13+ only")
@pytest.mark.parametrize("option_help", ["-h", "--help"])
def test_run_help(monkeypatch, capsys, option_help):
    monkeypatch.setenv("COLUMNS", "80")
    monkeypatch.setattr(
        "sys.argv",
        ["kafka-collector", option_help],
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run()
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == 0

    with open("tests/expected-output/cli-help.txt", "r") as f:
        expected_output = f.read()

    captured = capsys.readouterr()
    assert captured.out == expected_output


@pytest.mark.skipif(sys.version_info >= (3, 13), reason="Python <3.13 only")
@pytest.mark.parametrize("option_help", ["-h", "--help"])
def test_run_help312(monkeypatch, capsys, option_help):
    monkeypatch.setenv("COLUMNS", "80")
    monkeypatch.setattr(
        "sys.argv",
        ["kafka-collector", option_help],
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run()
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == 0

    with open("tests/expected-output/cli-help312.txt", "r") as f:
        expected_output = f.read()

    captured = capsys.readouterr()
    assert captured.out == expected_output


@pytest.mark.parametrize("option_version", ["-v", "--version"])
def test_run_show_version(monkeypatch, capsys, option_version):
    monkeypatch.setattr(
        "sys.argv",
        ["kafka-collector", option_version],
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run()
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == 0

    with open("tests/expected-output/cli-version.txt", "r") as f:
        expected_output = f.read()

    captured = capsys.readouterr()
    assert captured.out == expected_output


@pytest.mark.parametrize("options", [["-p"], ["-p", "-j"]])
def test_run_wrong_options(monkeypatch, capsys, options):
    monkeypatch.setattr(
        "sys.argv",
        ["kafka-collector", "-t", "xxx"] + options,
    )

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run()
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == 2

    captured = capsys.readouterr()
    assert "usage: kafka-collector [-h] [-v]" in captured.err
    assert "kafka-collector: error: unrecognized arguments:" in captured.err
