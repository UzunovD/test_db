import pytest

from data_base import DB


def execute_user_prompt(db, monkeypatch, user_prompt):
    monkeypatch.setattr('builtins.input', lambda _: user_prompt)
    user_input = db._get_user_input()
    action, msg = db._get_action(user_input)
    db._execute(action, user_input)


def test_get(monkeypatch, capsys):
    db = DB()
    execute_user_prompt(db, monkeypatch, "GET A")
    assert 'NULL\n' == capsys.readouterr().out


def test_set(monkeypatch):
    db = DB()
    execute_user_prompt(db, monkeypatch, "SET A 10")
    assert 'A' in db._db
    assert '10' in db._value_data
    assert '10' == db._db['A']


def test_set_get(monkeypatch, capsys):
    db = DB()
    execute_user_prompt(db, monkeypatch, "SET A 10")
    execute_user_prompt(db, monkeypatch, "GET A")
    assert '10\n' == capsys.readouterr().out


def test_counts(monkeypatch, capsys):
    db = DB()
    execute_user_prompt(db, monkeypatch, "SET A 10")
    execute_user_prompt(db, monkeypatch, "COUNTS 10")
    assert '1\n' == capsys.readouterr().out

    execute_user_prompt(db, monkeypatch, "SET C 10")
    execute_user_prompt(db, monkeypatch, "COUNTS 10")
    assert '2\n' == capsys.readouterr().out


def test_unset(monkeypatch, capsys):
    db = DB()
    execute_user_prompt(db, monkeypatch, "SET B 20")
    execute_user_prompt(db, monkeypatch, "UNSET B")

    execute_user_prompt(db, monkeypatch, "GET B")
    assert 'NULL\n' == capsys.readouterr().out


def test_end(monkeypatch):
    db = DB()
    with pytest.raises(KeyboardInterrupt):
        execute_user_prompt(db, monkeypatch, "END")


def test_transaction(monkeypatch, capsys):
    db = DB()
    execute_user_prompt(db, monkeypatch, "BEGIN")
    execute_user_prompt(db, monkeypatch, "SET A 10")
    execute_user_prompt(db, monkeypatch, "FIND 10")
    assert 'A\n' == capsys.readouterr().out
    execute_user_prompt(db, monkeypatch, "BEGIN")
    execute_user_prompt(db, monkeypatch, "SET A 20")
    execute_user_prompt(db, monkeypatch, "BEGIN")
    execute_user_prompt(db, monkeypatch, "SET A 30")
    execute_user_prompt(db, monkeypatch, "GET A")
    assert '30\n' == capsys.readouterr().out
    execute_user_prompt(db, monkeypatch, "ROLLBACK")
    execute_user_prompt(db, monkeypatch, "GET A")
    assert '20\n' == capsys.readouterr().out
    execute_user_prompt(db, monkeypatch, "COMMIT")
    execute_user_prompt(db, monkeypatch, "GET A")
    assert '20\n' == capsys.readouterr().out
    execute_user_prompt(db, monkeypatch, "BEGIN")
    execute_user_prompt(db, monkeypatch, "SET A 20")
    execute_user_prompt(db, monkeypatch, "COUNTS 20")
    assert '2\n' == capsys.readouterr().out

