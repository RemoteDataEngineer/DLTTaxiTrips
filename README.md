# DLTTaxiTrips
Crate Delta Live Tables in AWS using Taxi Datasets in Databricks
![delta live tables](https://github.com/RemoteDataEngineer/DLTTaxiTrips/assets/140629527/bc79ff3c-8fa8-4c9a-8c02-a119d468d0f8)

# Bronze
Read in Yellow and Green Taxi data from Databricks datasets

# Silver
Run exceptions from a dataframe to clean Silver dataset for Yellow and Green

# Gold
Union Yellow and Green. Create a Live Streaming SCD table for VendorID
