"""Behaviour of the Spark ETL, checked on the OMOP tables it produced."""


def concept_id(con, vocab, code):
    return con.execute("SELECT concept_id FROM cdm.concept WHERE vocabulary_id = ? AND concept_code = ?",
                       [vocab, code]).fetchone()[0]


def test_both_sites_loaded_without_id_collisions(con):
    sites = dict(con.execute("SELECT split_part(person_source_value, '|', 1), COUNT(*) FROM cdm.person "
                             "GROUP BY 1").fetchall())
    assert set(sites) == {"CVH", "RCH"}
    for t, pk in [("person", "person_id"), ("visit_occurrence", "visit_occurrence_id"),
                  ("measurement", "measurement_id"), ("condition_occurrence", "condition_occurrence_id")]:
        n, d = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT {pk}) FROM cdm.{t}").fetchone()
        assert n == d, f"duplicate {pk}"


def test_one_source_code_can_map_to_two_standard_concepts(con):
    targets = {r[0] for r in con.execute(
        "SELECT DISTINCT condition_concept_id FROM cdm.condition_occurrence WHERE condition_source_value = 'E11.649'"
    ).fetchall()}
    assert targets == {concept_id(con, "SNOMED", "44054006"), concept_id(con, "SNOMED", "302866003")}


def test_rows_are_routed_by_the_standard_concepts_domain(con):
    # ICD-10-CM Z87.891 is a "diagnosis" in the source but its standard concept is an Observation
    assert con.execute("SELECT COUNT(*) FROM cdm.observation WHERE observation_source_value = 'Z87.891'").fetchone()[0]
    assert not con.execute("SELECT COUNT(*) FROM cdm.condition_occurrence "
                           "WHERE condition_source_value = 'Z87.891'").fetchone()[0]
    # Lab-panel CPT codes land in MEASUREMENT, not PROCEDURE_OCCURRENCE
    assert con.execute("SELECT COUNT(*) FROM cdm.measurement WHERE measurement_source_value = '83036'").fetchone()[0]


def test_retired_icd_code_still_maps(con):
    assert con.execute("SELECT MIN(condition_concept_id) FROM cdm.condition_occurrence "
                       "WHERE condition_source_value = 'N18.3'").fetchone()[0] == concept_id(con, "SNOMED", "433144002")


def test_units_are_harmonised(con):
    rows = dict(con.execute("""
        SELECT m.unit_source_value, MEDIAN(m.value_as_number) FROM cdm.measurement m
        JOIN cdm.concept c ON c.concept_id = m.measurement_concept_id
        WHERE c.concept_code = '29463-7' GROUP BY 1""").fetchall())
    assert set(rows) == {"[oz_av]", "kg", "[lb_av]"}
    assert max(rows.values()) / min(rows.values()) < 1.05, "ounces, pounds and kg agree after conversion"
    stored = {r[0] for r in con.execute("""SELECT DISTINCT u.concept_code FROM cdm.measurement m
        JOIN cdm.concept c ON c.concept_id = m.measurement_concept_id JOIN cdm.concept u ON u.concept_id = m.unit_concept_id
        WHERE c.concept_code IN ('29463-7', '8302-2')""").fetchall()}
    assert stored == {"kg", "cm"}


def test_ifcc_hba1c_is_converted_to_percent(con):
    r = con.execute("""SELECT MIN(value_as_number), MAX(value_as_number), MIN(CAST(value_source_value AS DOUBLE))
        FROM cdm.measurement WHERE measurement_source_value = '59261-8'""").fetchone()
    if r[0] is not None:  # small runs may contain none
        assert 4 < r[0] and r[1] < 15 and r[2] > 20, "mmol/mol in the source, NGSP % in the CDM"


def test_blood_pressure_string_split_into_two_measurements(con):
    n_sys = con.execute("SELECT COUNT(*) FROM cdm.measurement WHERE measurement_source_value = '8480-6'").fetchone()[0]
    n_dia = con.execute("SELECT COUNT(*) FROM cdm.measurement WHERE measurement_source_value = '8462-4'").fetchone()[0]
    assert n_sys == n_dia > 0


def test_non_numeric_lab_results(con):
    ops = con.execute("SELECT COUNT(*) FROM cdm.measurement WHERE value_source_value = '>14.0' "
                      "AND value_as_number = 14 AND operator_concept_id IS NOT NULL").fetchone()[0]
    total = con.execute("SELECT COUNT(*) FROM cdm.measurement WHERE value_source_value = '>14.0'").fetchone()[0]
    assert ops == total
    assert not con.execute("SELECT COUNT(*) FROM cdm.measurement WHERE value_source_value = 'HEMOLYZED' "
                           "AND value_as_number IS NOT NULL").fetchone()[0]


def test_reviewed_local_mappings_are_used(con):
    via = dict(con.execute("SELECT mapped_via, SUM(records) FROM etl.source_coverage WHERE mapped GROUP BY 1").fetchall())
    assert via.get("source_to_concept_map", 0) > 0
    assert con.execute("SELECT COUNT(*) FROM etl.source_coverage WHERE NOT mapped").fetchone()[0] == 0


def test_every_person_has_an_observation_period(con):
    assert not con.execute("SELECT COUNT(*) FROM cdm.person p WHERE NOT EXISTS "
                           "(SELECT 1 FROM cdm.observation_period o WHERE o.person_id = p.person_id)").fetchone()[0]


def test_drug_eras_roll_up_to_ingredients(con):
    classes = {r[0] for r in con.execute("SELECT DISTINCT c.concept_class_id FROM cdm.drug_era d "
                                         "JOIN cdm.concept c ON c.concept_id = d.drug_concept_id").fetchall()}
    assert classes == {"Ingredient"}
