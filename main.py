import streamlit as st
import pandas as pd
import snowflake.connector
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session


#https://docs.streamlit.io/develop/api-reference/connections/st.connections.snowflakeconnection#configuration
#https://docs.streamlit.io/develop/tutorials/databases/snowflake


#Initial headers and connecting to snowflake
st.title("Data Sanity Check")
st.write("Establishing the Snowflake connection")
st.write("Update the snowflake login parameters in system local file path for toml file -->> C:\\Users\\<system_name>\\.snowflake\\connections.toml")

#session = get_active_session() #-->>use this as session in streamlit
session=Session.builder.create()


def schema_validation(schema1,db1,db2):
    query2=f"""select count(*) as cnt from snowflake.account_usage.schemata where catalog_name in ('{db1}','{db2}') and \
                                    schema_name in ('{schema1}') """
                
    print(query2)
    schema_check=session.sql(query2).to_pandas()
    print("Schema check value:",schema_check['CNT'][0])
    if (schema_check['CNT'][0])>0:
        return schema1
    else:
        raise ValueError("Incorrect Schema Name. Recheck it")
    
def database_validation(db):
    query1=f"select count(*) as cnt from snowflake.account_usage.databases where database_name in ('{db}')"
    print(query1)
    db_check=session.sql(query1).to_pandas()
    #print(db_check['CNT'][0])
    return db_check['CNT'][0]

def object_validation(db1,db2,schema1,schema2):  
    try:  
        db_v=database_validation(db1)
        db_v2=database_validation(db2)  
        schema1_val=schema_validation(schema1,db1,db2)
        schema2_val=schema_validation(schema2,db1,db2)
    except Exception as e:
        raise Exception("Revalidate the db,schma names") 

def input_table_name(db,schema,tbl):
    query=f"select count(*) as cnt from {db}.{schema}.{tbl}"
    print(query)
    exe1=session.sql(query).to_pandas()
    print("Records value:",exe1['CNT'][0])
    return exe1['CNT'][0]


def record_count_check(db1,schema1,tbl1,db2,schema2,tbl2):
    table1_row_count=input_table_name(db1,schema1,tbl1)
    table2_row_count=input_table_name(db2,schema2,tbl2)
    st.write(f"{db1}.{schema1}.{tbl1} table records count : {table1_row_count}")
    st.write(f"{db2}.{schema2}.{tbl2} table records count : {table2_row_count}")
    if table1_row_count==table2_row_count:
        # st.html("<b>Row count matched</b>")  
        st.markdown(":green[Row counts matched]")     
    else:
        st.markdown(":red[Row counts are not matching]")
        # st.write("Row counts are not matching")

    return table1_row_count,table2_row_count

def unique_records_check(db,schema,tbl,pk):
    print("pk value in function is :",pk)
    if pk!=1:
        query=f"select count(*) as cnt from (select * from {db}.{schema}.{tbl}  group by ALL)"
        unique_records_count=session.sql(query).to_pandas()
        rec=unique_records_count['CNT'][0]
    else:
        query=f"select count(*) as cnt from (select {pk},count(*) from {db}.{schema}.{tbl}  group by {pk})"
        print(query)
        unique_records_count=session.sql(query).to_pandas()
        rec=unique_records_count['CNT'][0]
    
    st.write(f"Unique records count in {db}.{schema}.{tbl} : {rec}")
    return rec


def display_duplicate_records(db,schema,tbl,pk_val,pk_input):
    if pk_val!=1:
        query=f"select a.*,count(*) from {db}.{schema}.{tbl} a  group by ALL having count(*)>1"
         #print(query)
        duplicate_data=session.sql(query).to_pandas()       
        st.write(f"Duplicate records display from {db}.{schema}.{tbl} table ")
        st.dataframe(duplicate_data)

    else:
        query=f"select {pk_input},count(*) from {db}.{schema}.{tbl}  group by {pk_input} having count(*)>1"
        #print(query)
        duplicate_data=session.sql(query).to_pandas()      
        st.write(f"Duplicate records display from {db}.{schema}.{tbl} table ")
        st.dataframe(duplicate_data)

    return duplicate_data

def ddl_extract(db,schema,tbl):
    import re
    query=f"""select get_ddl('table','{db}.{schema}.{tbl}')"""
    query_ddl=session.sql(query).collect()
    split_pattern=f"{tbl}".lower()

    # new_extract11='('+str(re.split(split_pattern + '.*\(',str(query_ddl).lower())[-1])

    new_extract='('+str(re.split(split_pattern + '.?\(',str(query_ddl).lower())[-1]).replace("\\n","").replace("\\t","").split(";")[0]
    st.write(new_extract)
    return new_extract
 
def data_match(db1,schema1,tbl1,db2,schema2,tbl2,pk_val,pk_input):
    #if pk_val!=1:

        #A-B
    query=f"""select a.*from {db1}.{schema1}.{tbl1} a 
                minus 
                select b.* from {db2}.{schema2}.{tbl2} b """
    #print(query)
    duplicate_data=session.sql(query).to_pandas()       
    st.write(f"Difference in records between ({db1}.{schema1}.{tbl1}) - ({db2}.{schema2}.{tbl2}) tables")
    st.dataframe(duplicate_data)  

    #B-A
    query2=f"""select a.*from {db2}.{schema2}.{tbl2} a 
                minus 
                select b.* from {db1}.{schema1}.{tbl1} b """
    #print(query2)
    duplicate_data2=session.sql(query).to_pandas()       
    st.write(f"Difference in records between ({db2}.{schema2}.{tbl2}) - ({db1}.{schema1}.{tbl1}) tables")
    st.dataframe(duplicate_data2)


#User inputs for DB,SCHEMA names
try:
    db1=str(st.text_input("Enter the 1st DB name"))
    db2=str(st.text_input("Enter the 2nd DB name"))   
    schema1=st.text_input("Enter the 1st Schema name")
    schema2=st.text_input("Enter the 2nd Schema name")
   
except Exception as e:   
    raise Exception("Please check the Schema name")


#validating and confirming the user inputs
tmp_button = st.button("Confirm  inputs")
if tmp_button:
    object_validation(db1,db2,schema1,schema2)
    st.session_state = True



if st.session_state:
    tbl1=st.text_input(f"{db1}.{schema1} object table input for table1")
    tbl2=st.text_input(f"{db2}.{schema2} object table input for table2")
    pk_val=st.number_input("Do you know the Primary Key column of table if yes then 1 else 0",0,1,1)

    if pk_val==1:          
        pk_input=st.text_input("Input the primary_key value for table") 

    else:
        pk_input=None

    tmp_button = st.button("Submit")
    if tmp_button:
        st.subheader("Test Case-1: Records Count")                              
        table1_row_count,table2_row_count=record_count_check(db1,schema1,tbl1,db2,schema2,tbl2)

        st.subheader("Test Case-2: Unique Records Count")
        tbl1_unique_records_count=unique_records_check(db1,schema1,tbl1,pk_input)
        tbl2_unique_records_count=unique_records_check(db2,schema2,tbl2,pk_input)


        if table1_row_count>tbl1_unique_records_count:
            st.subheader("Test Case-3: Duplicate Record Check")
            display_duplicate_records(db1,schema1,tbl1,pk_val,pk_input)
            #st.markdown(f'<p style="background-color:#0066cc;color:#33ff33;font-size:24px;border-radius:2%;">{"Failed"}</p>', unsafe_allow_html=True)
            st.markdown(":red[Dulicate check test- Failed]")
        elif table2_row_count>tbl2_unique_records_count:
            st.subheader("Test Case-3: Duplicate Records Check")
            display_duplicate_records(db2,schema1,tbl2,pk_val,pk_input)
            st.markdown(":red[Dulicate check test- Failed]")

        elif tbl2_unique_records_count!=tbl1_unique_records_count:
            st.markdown(":red[Missing records:-Record counts are not same in Table1 and Table2]")
        
        else:
            st.markdown(":green[No duplicate records found in tables]")
            # st.html("No Duplicate records found in any of tables selected - <b>Pass</b>")
        
        st.subheader("Test Case-4: Comparison of DDLs")
        tbl1_ddl=ddl_extract(db1,schema1,tbl1)
        tbl2_ddl=ddl_extract(db2,schema2,tbl2)
    
        if tbl1_ddl==tbl2_ddl:
            st.markdown(":green[Tables DDLs are matching]") 
        else:
            st.markdown(":red[Tables DDLs are not matching]") 

        #A-B and B-A data checking
        st.subheader("Test Case: Differences in table data")
        diff_check=st.number_input("Do you want check the differences in Data for tables if yes then 1 else 0",0,1,0)
        if diff_check==1:
            st.subheader("Test Case-5: Difference in data between tables")
            data_match(db1,schema1,tbl1,db2,schema2,tbl2,pk_val,pk_input)
        else:
            st.write("Good to go")
        


    else:
        st.write("Input the table_names")
st.stop()
