# Databricks notebook source
# MAGIC %run ./../Transformer/Trannsformer_2_Write_Blob $input_param_name="Parameter Passed from Parent"

# COMMAND ----------

# https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-quickstart-create-databricks-account
# https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-datalake-gen2
# https://docs.microsoft.com/en-us/azure/storage/common/storage-auth-aad-app?tabs=dotnet#register-your-application-with-an-azure-ad-tenant
# https://stackoverflow.com/questions/52769758/azure-blob-storage-authorization-permission-mismatch-error-for-get-request-wit# 
# Use a service principal directly.
TenantId_DirectoryId=dbutils.secrets.get(scope = "databricks_amar_secret_scope", key = "TenantId-DirectoryId") # a5add562-f78e-4ed6-afb6-5154ceb18f17
clientId_PrincipalId=dbutils.secrets.get(scope = "databricks_amar_secret_scope", key = "clientId-PrincipalId") # 96cfb0b7-6288-441f-aa25-9bdfb7d93de6
clientSecret=dbutils.secrets.get(scope = "databricks_amar_secret_scope", key = "clientSecret") # 8UFpYGS71a_QDHA5U_CJ9hgq7~.MBGq~Ox

spark.conf.set("fs.azure.account.auth.type.adls2amardeepstorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adls2amardeepstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# Azure AD  provides a client ID (also called an application ID) that you use to associate your application with Azure AD at runtime.
# portal --> app-registrations --> API permissions --> Grant your registered app permissions to Azure Storage
spark.conf.set("fs.azure.account.oauth2.client.id.adls2amardeepstorage.dfs.core.windows.net", clientId_PrincipalId)
# app-registrations --> Certificates & secrets  --> Create a client secret
spark.conf.set("fs.azure.account.oauth2.client.secret.adls2amardeepstorage.dfs.core.windows.net", clientSecret)
#spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account-name>.dfs.core.windows.net", "https://login.microsoftonline.com/<tenant-id>/oauth2/token")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adls2amardeepstorage.dfs.core.windows.net", "https://login.microsoftonline.com/"+TenantId_DirectoryId+"/oauth2/token")


adls_path_url="abfss://adlsgen2-container@adls2amardeepstorage.dfs.core.windows.net/tables/ParquetFile"
# dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/")
dbutils.fs.ls("abfss://adlsgen2-container@adls2amardeepstorage.dfs.core.windows.net/tables/ParquetFile")

spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dataF = spark.read.parquet(adls_path_url)
dataF.show(4)
dataF.explain()
dataF.createOrReplaceTempView("sql_table")
spark.sql("select * from sql_table limit 200").show()
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

#Transform
dataF_Transformed=fnTransformDataFrame(spark,dataF)


#Write
adls_write_folder="Write_Results/Parquet"
adls_container="adlsgen2-container"
adls_account_name="adls2amardeepstorage"
adls_write_path_url = "abfss://{}@{}.dfs.core.windows.net/{}".format(adls_container, adls_account_name,adls_write_folder)
lstPartitionCol=["current_date"]
fnWriteJsonToBlob(spark,dataF_Transformed,adls_write_path_url,lstPartitionCol)
dbutils.notebook.exit("True")