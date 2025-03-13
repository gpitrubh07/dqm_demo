import pandas as pd
import great_expectations as gx
import json

df = pd.read_csv("v_dqm_kpi.csv")
print(df)

context = gx.get_context()

# Retrieve the Data Source
data_source = context.data_sources.add_pandas("pandas62")

# Add a Data Asset to the Data Source
data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")

batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
batch = batch_definition.get_batch(batch_parameters={"dataframe":df})

# expectation = gx.expectations.ExpectColumnValuesToBeBetween(
    # column="enrollment_cnt",
    # min_value=0,
    # max_value=25
# )
# expectation1 = gx.expectations.ExpectColumnToExist(
    # column="PHARMACY",
    # column_index=1
# )
expectations = [
    gx.expectations.ExpectColumnValuesToBeBetween(column="enrollment_cnt",min_value=0,max_value=25),
    gx.expectations.ExpectColumnToExist(column="PHARMACY",column_index=1),
    gx.expectations.ExpectColumnMeanToBeBetween(column="enrollment_cnt",min_value=0,max_value=26),
    gx.expectations.ExpectColumnMaxToBeBetween(column="enrollment_cnt",min_value=0,max_value=26),
    gx.expectations.ExpectColumnValuesToBeNull(column="enrollment_cnt",mostly=0),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="enrollment_cnt",mostly=0)
]

# expectation2 = gx.expectations.ExpectColumnMeanToBeBetween(
#     column="enrollment_cnt",
#     min_value=0,
#     max_value=26
# )
# expectation3 = gx.expectations.ExpectColumnMaxToBeBetween(
#     column="enrollment_cnt",
#     min_value=0,
#     max_value=26
# )
# expectation4 = gx.expectations.ExpectColumnValuesToBeNull(
#     column="startform_cnt",
#     mostly=0.66
# )
# expectation5 = gx.expectations.ExpectColumnValuesToNotBeNull(
#     column="startform_cnt",
#     mostly=0.66
# )


#validation_result_list = validation_result.to_json_dict()["results"]
# validation_result = batch.validate(expectation)
# print("validation_result\n:",validation_result)
# validation_result1 = batch.validate(expectation1)
# print(validation_result1)


#validation_result_list = validation_result.to_json_dict()
#print("\nvalidation_json\n:",validation_json)
#
results_data = {}

for i,expectation in enumerate(expectations, start=1):
    validation_result = batch.validate(expectation)
    validation_result.to_json_dict()
    res_key = validation_result.keys()
    res_val = validation_result.values()
    #print(validation_result)
    
    validation_result_list = validation_result.to_json_dict()
    
    # for key,value in validation_result_list.items():
    #     try:
    #         if key == 'success':
    #                 results_data[f"success_{i}"] = value
    #         elif key =='expectation_config' and isinstance(value, dict):
    #                 results_data[f"expectation_type_{i}"] = value["type"]
    #                 results_data[f"column_{i}"] = value["kwargs"]["column"]
    #                 results_data[f"min_value_{i}"] = value["kwargs"]["min_value"]
    #                 results_data[f"max_value_{i}"] = value["kwargs"]["max_value"]
    #         elif key=='result' and isinstance(value, dict):
    #                 results_data[f"result_{i}"] = value.get('element_count',None)
    #                 results_data[f"unexpected_count_{i}"] = value.get('unexpected_count',None)
    #                 results_data[f"observed_value_{i}"] = value.get('observed_value',None)
    #     except:
    #         pass

    for key,value in validation_result_list.items():
        try:
            if key == 'success':
                        results_data[f"success"] = value
            elif key =='expectation_config' and isinstance(value, dict):
                        results_data[f"expectation_type"] = value["type"]
                        results_data[f"column"] = value["kwargs"]["column"]
                        results_data[f"min_value"] = value["kwargs"]["min_value"]
                        results_data[f"max_value"] = value["kwargs"]["max_value"]
            elif key=='result' and isinstance(value, dict):
                        results_data[f"result"] = value.get('element_count',None)
                        results_data[f"unexpected_count"] = value.get('unexpected_count',None)
                        results_data[f"observed_value"] = value.get('observed_value',None)
        except:
            pass

print(validation_result)
print("------XXXX-----")
# print(results_data)
print(res_key)
print("------XXXX-----")
print(res_val)

# validation_df = pd.DataFrame([results_data])
# validation_df.to_csv("validation_result.csv", header=True, sep=',', mode='a', index=False, encoding='utf-8')

#validation_df.to_csv("validation_result.csv",index=False)
# print(validation_df)


# validation_df.to_excel("validation_result.xlsx",index=False, engine="openpyxl")
# print(validation_df)