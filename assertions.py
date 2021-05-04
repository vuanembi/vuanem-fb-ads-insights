def assertion(res):
    assert res["results"]["num_processed"] > 0
    assert res["results"]["output_rows"] > 0
    assert res["results"]["num_processed"] == res["results"]["output_rows"]
    assert res["results"]["errors"] is None
