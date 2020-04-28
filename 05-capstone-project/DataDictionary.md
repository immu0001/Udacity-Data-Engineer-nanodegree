# DEND Capstone

# **Data Dictionary Dimension Tables**
## Airports Data
 * ident: string (nullable = true) - Airport id
 * type: string (nullable = true) - size of airport
 * name: string (nullable = true) - name
 * elevation_ft: float (nullable = true) - elevation in feet
 * continent: string (nullable = true) - continet
 * iso_country: string (nullable = true) - country (ISO-2)
 * iso_region: string (nullable = true) - region (ISO-2)
 * municipality: string (nullable = true) - municipality
 * gps_code: string (nullable = true) - gps code
 * iata_code: string (nullable = true) - IATA code
 * local_code: string (nullable = true) - Local code
 * coordinates: string (nullable = true) - coordinates
 
## U.S. Demographic by State
 * State: string (nullable = true)-Full state name
 * state_code: string (nullable = true)- State code
 * Total_Population: double (nullable = true) - Total population of the state
 * Male_Population: double (nullable = true)- Total Male population per state
 * Female_Population: double (nullable = true)- Total Female population per state
 * American_Indian_and_Alaska_Native: long (nullable = true) - Total American Indian and Alaska Native population per state
 * Asian: long (nullable = true) - Total Asian population per state
 * Black_or_African-American: long (nullable = true) - Total Black or African-American population per state
 * Hispanic_or_Latino: long (nullable = true) - Total Hispanic or Latino population per state 
 * White: long (nullable = true) - Total White population per state 
 * Male_Population_Ratio: double (nullable = true) - Male population ratio per state
 * Female_Population_Ratio: double (nullable = true) - Female population ratio per state
 * American_Indian_and_Alaska_Native_Ratio: double (nullable = true) - Black or African-American population ratio per state
 * Asian_Ratio: double (nullable = true) - Asian population ratio per state
 * Black_or_African-American_Ratio: double (nullable = true) - Black or African-American population ratio per state
 * Hispanic_or_Latino_Ratio: double (nullable = true) - Hispanic or Latino population ratio per state 
 * White_Ratio: double (nullable = true) - White population ratio per state 
 
## Airlines
 * Airline_ID: integer (nullable = true) - Airline id
 * Name: string (nullable = true) -  Airline name
 * IATA: string (nullable = true) - IATA code
 * ICAO: string (nullable = true) - ICAO code
 * Callsign: string (nullable = true) - name code
 * Country: string (nullable = true) - country
 * Active: string (nullable = true) - Active

## Countries
 * cod_country: long (nullable = true) - Country code
 * country_name: string (nullable = true) - Country name
 

## Visas
 * cod_visa: string (nullable = true) - visa code
 * visa: string (nullable = true) - visa description
 
## Mode to access
 * cod_mode: integer (nullable = true) - Mode code
 * mode_name: string (nullable = true) - Mode description


# Fact Table (Inmigration Registry)
 * cic_id: integer (nullable = true) - CIC id
 * cod_port: string (nullable = true) - Airport code
 * cod_state: string (nullable = true) - US State code
 * visapost: string (nullable = true) - Department of State where where Visa was issued
 * matflag: string (nullable = true) - Match flag - Match of arrival and departure records
 * dtaddto: string (nullable = true) -  Character Date Field - Date to which admitted to U.S. (allowed to stay until)
 * gender: string (nullable = true) - Gender
 * airline: string (nullable = true) - Airline code
 * admnum: double (nullable = true) - Admission Number
 * fltno: string (nullable = true) - Flight number of Airline used to arrive in U.S.
 * visatype: string (nullable = true) - Class of admission legally admitting the non-immigrant to temporarily stay in U.S
 * cod_visa: integer (nullable = true) - Visa code
 * cod_mode: integer (nullable = true) - Mode code
 * cod_country_origin: integer (nullable = true) - Country of origin code
 * cod_country_cit: integer (nullable = true) - City code of origin
 * year: integer (nullable = true) - Year
 * month: integer (nullable = true) - Month
 * bird_year: integer (nullable = true) - Year of Birth
 * age: integer (nullable = true) - Age
 * counter: integer (nullable = true) - Used for summary statistics
 * arrival_date: date (nullable = true) - Arrival date
 * departure_date: date (nullable = true) - Departure Date
 * arrival_year: integer (nullable = true) - arrival year
 * arrival_month: integer (nullable = true) - Arrival month
 * arrival_day: integer (nullable = true) - arrival day of month