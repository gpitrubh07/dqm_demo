import pandas as pd
import great_expectations as gx
import json
import uuid

df = pd.read_csv("v_dqm_kpi.csv")
print(df)

context = gx.get_context()

# Retrieve the Data Source
data_source = context.data_sources.add_pandas(str(uuid.uuid4()))

# Add a Data Asset to the Data Source
data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")

batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
batch = batch_definition.get_batch(batch_parameters={"dataframe":df})

expectations = [
    gx.expectations.ExpectColumnValuesToBeBetween(column="enrollment_cnt",min_value=0,max_value=25),
    gx.expectations.ExpectColumnToExist(column="PHARMACY",column_index=1),
    gx.expectations.ExpectColumnMeanToBeBetween(column="enrollment_cnt",min_value=0,max_value=26),
    gx.expectations.ExpectColumnMaxToBeBetween(column="enrollment_cnt",min_value=0,max_value=26),
    gx.expectations.ExpectColumnValuesToBeNull(column="enrollment_cnt",mostly=0),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="enrollment_cnt",mostly=0)
]


results_list = []

for i,expectation in enumerate(expectations, start=1):
    validation_result = batch.validate(expectation)
    print(validation_result)
    
    validation_result_dict = validation_result.to_json_dict()
    
    row_data = {
		"success": validation_result_dict.get("success",None),
        "Expectation Type": validation_result_dict.get("expectation_config",{}).get("type",None),
        "column_name": validation_result_dict.get("expectation_config",{}).get("kwargs",{}).get("column",None),
        "min_value": validation_result_dict.get("expectation_config",{}).get("kwargs",{}).get("min_value",None),
        "max_value": validation_result_dict.get("expectation_config",{}).get("kwargs",{}).get("max_value",None),
        "result": validation_result_dict.get("result",{}).get("element_count",None),
        "Unexpected Count": validation_result_dict.get("result",{}).get("unexpected_count", None),
        "observed_value": validation_result_dict.get("result",{}).get("observed_value", None),
	}
	
    results_list.append(row_data)

validation_df = pd.DataFrame(results_list)

validation_df.to_csv("validation_result1.csv",index=False)
print(validation_df)

# # validation_df.to_excel("validation_result.xlsx",index=False, engine="openpyxl")
# print("\nValidation Results:\n")
# print(validation_df.to_string(index=False))