# Odins Migration Suite 
### Version 0.0.1-Alpha

## Description
This small application is used for performing data migration based on csv files.
In the actual version the following functions are supported:
- Create a transformation reference for keys. You can choose between csv or sqlite db output
- You can perform simple transformations. Copy entries, exchange data with data from a reference, exchange fields with constants


## Usage

For performing an operation you have to create a jobcontrol file and execute it with 
the odinmigrator.py 

Example:  
odinmigrator.py ./test/jobtransformation1.yml

### Jobcontroll Files

## Create reference example
For creating a reference you can choose between the import of a reference or the generating 
of a reference.

### Generation of new values for saving data into a sqlite DB
````
job_name: "Create reference job"
job_type: createReference
parameter:
  reference_name: "testref"
  input_data: "test/testrefinput1.csv"
  creation_rule: 1
  start_value: 100000
  increment: 1
  field_type: BIGINT
  negate: True
  output_type: db
  target_file: "d:\\ref.db"
````
Parameter| Description 
----|----
job_name| Name of the job
job_type | for creating a reference this value must be 'createReference'
parameter | 
 reference_name| name of the reference and the corresponding table name
 input_data | csv input file with the old_keys.The file must have a header row with the value 'Input' to identify the old key
 creation_rule | 1 = Generate 2 = import             
 start_value  | startvalue for generating the new key
 increment | increment used during generating the new key
 field_type | Type of the columns of the reference table . Values BIGINT or TEXT
 negate | Flag indicating if the new value will be multiplied with -1 or not
 output_type | db for database , csv for creation of files
 target_file | path to the target database file. if it does not exists it will be created


Attention: if a reference already exists in the database , the data will be truncated and the new data will be inserted

### Importing a reference into a sqlite DB
````
job_name: "Create reference job"
job_type: createReference
parameter:
  reference_name: "testref"
  input_data: "test/testrefinput2.csv"
  creation_rule: 2
  field_type: BIGINT
  target_file: "d:\\ref.db"
````
Parameter| Description 
----|----
job_name| Name of the job
job_type | for creating a reference this value must be 'createReference'
parameter | 
 reference_name| name of the reference and the corresponding table name
 input_data | csv input file with the old_keys.The file must have a header row with the value 'Input' to identify the old key
 creation_rule | 1 = Generate 2 = import
 field_type | Type of the columns of the reference table . Values BIGINT or TEXT
 target_file | path to the target database file. if it does not exists it will be created


Attention: if a reference already exists in the database , the data will be truncated and the new data will be inserted

### Generation of new values for saving data into a csv file
````
job_name: "Create reference job"
job_type: createReference
parameter:
  reference_name: "testref"
  input_data: "test/testrefinput1.csv"
  creation_rule: 1
  start_value: 100000
  increment: 1
  negate: False
  output_type: csv
  target_file: "d:\\ref.csv"
````
Parameter| Description 
----|----
job_name| Name of the job
job_type | for creating a reference this value must be 'createReference'
parameter | 
 reference_name| name of the reference and the corresponding table name
 input_data | csv input file with the old_keys.The file must have a header row with the value 'Input' to identify the old key
 creation_rule | 1 = Generate 2 = import             
 start_value  | startvalue for generating the new key
 increment | increment used during generating the new key
 field_type | Type of the columns of the reference table . Values BIGINT or TEXT
 negate | Flag indicating if the new value will be multiplied with -1 or not
 output_type | db for database , csv for creation of files
 target_file | path to the target database file. if it does not exists it will be created

Attention: If a file already exists, it will be overridden


## Transformation of data
````
job_name: "Transformation Tabelle 1"
job_type: transformation
parameter:
  input_data: "test/testtransform1.csv"
  target_file: "d:\\output.csv"
  reference_database: "test/test.db"
  transformation_rules:
      - fieldpos: 0
        sourcefield: 0
        rule: copy
      - fieldpos: 1
        sourcefield: 1
        rule: ref
        reference_name: testref
      - fieldpos: 2
        rule: const
        value: "33"
````
Parameter| Description 
----|----
job_name| Name of the job
job_type | for transformation jobs it must be 'transformation'
parameter | 
 input_data | csv input file with the original data
 target_file | target file to save the result
 reference_database | path to the reference database to be used during the transformation
  transformation_rules | rules used during the transformation. must be provided for every output field
 fieldpos  | index of the taget column
 sourcefield | index of the source coloumn
 rule | copy , ref or const
 reference_name | if rule = ref this is the name of the reference_name used for getting the new key
 value  | if rule = const this is the field vor the constant value
 
During the transformation it can happen that a row could not be migrated. In this case the inputdata plus error informations
are stored in an error file.
Actual is this the case if no reference is found in the database. After correcting the reference you can delete the last column 
of the error file and use the normal transformation function to get missing transformations

At the end of a transformation a small result with input, output and error count is displayed


## Job protocol
In the job protocol you can see the startend, endtime, the duration and the cpu time. 
````
2022-02-13 16:06:02,784 - Odin Migrator Suite - Transformation - INFO - Rows read 2 - transformation successful : 1 - transformation in error: 1
2022-02-13 16:06:02,788 - Odin Migrator Suite - main - INFO - ========================================================
2022-02-13 16:06:02,788 - Odin Migrator Suite - main - INFO - Job statistics : 
2022-02-13 16:06:02,788 - Odin Migrator Suite - main - INFO - Job started at : 2022-02-13 16:06:02.776488
2022-02-13 16:06:02,788 - Odin Migrator Suite - main - INFO - Job ended   at : 2022-02-13 16:06:02.788489
2022-02-13 16:06:02,789 - Odin Migrator Suite - main - INFO - Duration       : 0:00:00.012001  
2022-02-13 16:06:02,789 - Odin Migrator Suite - main - INFO - CPU TIME       : 0.171875  
2022-02-13 16:06:02,789 - Odin Migrator Suite - main - INFO - Job ended with status SUCCESS
````
