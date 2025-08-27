from etl.resolver import canonicalize, load_aliases
def test_rules_and_aliases(tmp_path):
    csvp = tmp_path / "aliases.csv"
    csvp.write_text("make,model,canonical_make,canonical_model\nFord,Fiesta Zetec,Ford,Fiesta\n", encoding="utf-8")
    load_aliases(str(csvp))
    mk, md = canonicalize("Ford", "Fiesta Zetec S 1.0 Ecoboost")
    assert mk == "ford" and md == "fiesta"
