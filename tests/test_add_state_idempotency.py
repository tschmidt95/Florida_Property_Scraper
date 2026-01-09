from scripts import add_state


def test_add_state_idempotent(tmp_path):
    base = tmp_path / "repo"
    routers_dir = base / "src" / "florida_property_scraper" / "routers"
    routers_dir.mkdir(parents=True, exist_ok=True)
    (routers_dir / "registry.py").write_text(
        'from florida_property_scraper.routers import fl\n\n_ROUTERS = {\n    "fl": fl,\n}\n\n',
        encoding="utf-8",
    )
    (routers_dir / "__init__.py").write_text(
        'from . import fl\n\n__all__ = ["fl"]\n', encoding="utf-8"
    )

    add_state.scaffold_state(base, "tx", "Texas")
    before = (routers_dir / "tx.py").read_text(encoding="utf-8")
    add_state.scaffold_state(base, "tx", "Texas")
    after = (routers_dir / "tx.py").read_text(encoding="utf-8")
    assert before == after
