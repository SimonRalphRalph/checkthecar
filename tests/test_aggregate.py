import pandas as pd
from etl.aggregate_mot import compute_aggregates
def test_aggregates_smoke():
    df = pd.DataFrame([
        {"make":"Ford","model":"Fiesta","firstUseDate":"2013-06-01","testDate":"2023-05-10",
         "odometerReading":72000,"odometerReadingUnits":"miles","testResult":"PASS","rfrAndComments":"","fuelType":"Petrol"},
        {"make":"Ford","model":"Fiesta","firstUseDate":"2013-06-01","testDate":"2024-05-11",
         "odometerReading":79000,"odometerReadingUnits":"miles","testResult":"FAIL","rfrAndComments":"BRS123 failure","fuelType":"Petrol"},
    ])
    out = compute_aggregates(df)
    assert "Ford" in out and "Fiesta" in out["Ford"]
    assert 2013 in out["Ford"]["Fiesta"]
