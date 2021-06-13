import requests
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
import gspread
from gspread_dataframe import set_with_dataframe

URL = "http://tarea-4.2021-1.tallerdeintegracion.cl/gho_{}.xml"

def generate_XML(country):
    XML_Data = requests.get(URL.format(country)).content
    return XML_Data            

countries = ['IND', 'ISR', 'HUN', 'BGD', 'DOM', 'HRV']

indicadores = ["Number of deaths", "Number of infant deaths", "Number of under-five deaths", 
           "Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)",
          "Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)",
           "Estimates of number of homicides",
           "Crude suicide rates (per 100 000 population)",
           "Mortality rate attributed to unintentional poisoning (per 100 000 population)",
           "Number of deaths attributed to non-communicable diseases, by type of disease and sex",
           "Estimated road traffic death rate (per 100 000 population)",
           "Estimated number of road traffic deaths",
           "Mean BMI (kg/m&#xb2;) (crude estimate)",
           "Mean BMI (kg/m&#xb2;) (age-standardized estimate)",
           "Prevalence of obesity among adults, BMI &GreaterEqual; 30 (age-standardized estimate) (%)",
           "Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)",
           "Prevalence of overweight among adults, BMI &GreaterEqual; 25 (age-standardized estimate) (%)",
           "Prevalence of overweight among adults, BMI &GreaterEqual; 25 (crude estimate) (%)",
           "Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)",
           "Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%)",
           "Prevalence of underweight among adults, BMI < 18.5 (crude estimate) (%)",
           "Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)",
           "Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)",
           "Prevalence of obesity among adults, BMI &GreaterEqual; 30 (crude estimate) (%)",
           "Estimate of daily cigarette smoking prevalence (%)",
           "Estimate of daily tobacco smoking prevalence (%)",
           "Estimate of current cigarette smoking prevalence (%)",
           "Estimate of current tobacco smoking prevalence (%)",
           "Mean systolic blood pressure (crude estimate)",
           "Mean fasting blood glucose (mmol/l) (crude estimate)",
           "Mean Total Cholesterol (crude estimate)"
          ]

def xml2df(XML_Data):
    root = ET.XML(XML_Data) # element tree
    all_records = []
    for i, child in enumerate(root):
        record = {}
        for subchild in child:
            if subchild.tag == "COUNTRY" or subchild.tag == "GHO" or subchild.tag == "YEAR" or subchild.tag == "SEX" or subchild.tag == "Numeric" or subchild.tag == "AGEGROUP" or subchild.tag == "GHECAUSES":
                record[subchild.tag] = subchild.text
        all_records.append(record)
    df = pd.DataFrame(all_records)

    return df

def generate_df_country():
    dfs = []
    for country in countries:
        XML_Data = generate_XML(country)
        df = xml2df(XML_Data)
        df = df[df.GHO.isin(indicadores)]
        df.reset_index(drop=True, inplace=True)

        dfs.append(df)
    return dfs

dfs = generate_df_country()
dfs = pd.concat(dfs)
dfs['Numeric'] = dfs['Numeric'].astype(float)
dfs['Numeric'] = np.where((dfs['GHO']=="Mortality rate attributed to unintentional poisoning (per 100 000 population)") | (dfs['GHO']=="Estimated road traffic death rate (per 100 000 population)") | (dfs['GHO']=="Crude suicide rates (per 100 000 population)") , 100000 * dfs['Numeric'], dfs['Numeric'])
dfs['YEAR2'] = pd.to_datetime(dfs['YEAR']).dt.date


gc = gspread.service_account(filename="taller-tarea-4-316223-c35e58a7c888.json")
sh = gc.open_by_key("1Cc__NtRfQqBfCwDL-b2QMDLoww758HeNn7TMGgIqS8U")
worksheet = sh.get_worksheet(0)
worksheet.clear()

set_with_dataframe(worksheet,dfs)



