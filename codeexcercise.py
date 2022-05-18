#!/usr/bin/env python
# coding: utf-8

# ## codeexcercise
# 
# 
# 

# In[3]:


json_data = [{"Gender":"Male","Heightcm":171,"weight":96},
{"Gender":"Male","Heightcm":161,"weight":85},
{"Gender":"Male","Heightcm":180,"weight":77},
{"Gender":"Female","Heightcm":166,"weight":62},
{"Gender":"Female","Heightcm":150,"weight":70},
{"Gender":"female","Heightcm":167,"weight":82}]


# In[4]:


#df = spark.read.option('multiline','true')\
               #.json(json_data)
#df.show()

df = spark.createDataFrame(json_data,['Gender','Height','weight'])


# In[5]:


display(df)


# In[6]:


from pyspark.sql.functions import lit

df_with_bmi  = df.withColumn("BMI",lit(df.weight/((df.Height*0.01)*(df.Height*0.01))))
display(df_with_bmi)


# 

# In[7]:


from decimal import Decimal

ref_data = [{"BMIcategory":"underweight","less":Decimal(0),"greater":Decimal(18.4),"Healthrisk":"malnutrition"},
{"BMIcategory":"Normalweight","less":Decimal(18.5),"greater":Decimal(24.9),"Healthrisk":"lowrisk"},
{"BMIcategory":"overweight","less":Decimal(25),"greater":Decimal(29.9),"Healthrisk":"Enhancedrisk"},
{"BMIcategory":"moderately obese","less":Decimal(30),"greater":Decimal(34.9),"Healthrisk":"Meduimrisk"},
{"BMIcategory":"severly obese","less":Decimal(35),"greater":Decimal(39.9),"Healthrisk":"highrisk"},
{"BMIcategory":"very severly obese","less":Decimal(40),"greater":Decimal(100),"Healthrisk":"very high risk"}]


# In[8]:


ref_df = spark.createDataFrame(ref_data)
display(ref_df)


# In[9]:


from pyspark.sql.functions import udf, struct, col
from pyspark.sql.types import * 
import pyspark.sql.functions as func


# In[10]:




def ref_fun1(x):
    if 0.0<=x<=18.4:
        return "underweight"
    if 18.5<=x<=24.9:
        return "Normalweight"
    if 25<=x<=29.9:
        return "overweight"
    if 30<=x<=34.9:
        return "moderatley obese" 
    if 35<=x<=39.9:
        return "severly obese" 
    if 40.0<=x<=100.0:
        return "very severly obese"           


                

                          
           

   
    
    
   
                      
            
           


# In[11]:



ref_fun_udf1 = udf(lambda z:ref_fun1(z),StringType())


# In[12]:


df1 = df_with_bmi.withColumn("BMIcategory",ref_fun_udf1(col("BMI")))

display(df1)


# In[13]:


def ref_fun2(x):
    if 0.0<=x<=18.4:
        return "malnutrition"
    if 18.5<=x<=24.9:
        return "lowrisk"
    if 25<=x<=29.9:
        return "Enhancedrisk"
    if 30<=x<=34.9:
        return "Medium risk" 
    if 35<=x<=39.9:
        return "high risk" 
    if 40.0<=x<=100.0:
        return "very high risk"


# In[14]:


ref_fun_udf2 = udf(lambda z:ref_fun2(z),StringType())


# In[15]:


final_df = df1.withColumn("Healthrisk",ref_fun_udf2(col("BMI")))

display(final_df)


# In[16]:


final_df.createOrReplaceTempView('bmidata')


# In[18]:


get_ipython().run_cell_magic('sql', '', "\nselect count(*) from bmidata where BMIcategory = 'overweight'")

