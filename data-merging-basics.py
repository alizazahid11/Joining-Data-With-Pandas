# Merge taxi_owners with taxi_veh on the column vid, and save the result to taxi_own_veh.
# Merge the taxi_owners and taxi_veh tables
taxi_own_veh = taxi_owners.merge(taxi_veh,on='vid')
# Print the column names of the taxi_own_veh
print(taxi_own_veh.columns)

# Set the left and right table suffixes for overlapping columns of the merge to _own and _veh, respectively.
taxi_own_veh = taxi_owners.merge(taxi_veh, on='vid', suffixes=('_own','_veh'))
print(taxi_own_veh.columns)

# Select the fuel_type column from taxi_own_veh and print the value_counts() to find the most popular fuel_types used.
print(taxi_own_veh['fuel_type'].value_counts())

# Merge wards and census on the ward column and save the result to wards_census.
wards_census = wards.merge(census,on='ward')
print('wards_census table shape:', wards_census.shape)

# Merge the wards_altered and census tables on the ward column, and notice the difference in returned rows.
# Print the first few rows of the wards_altered table to view the change 
print(wards_altered[['ward']].head())
# Merge the wards_altered and census tables on the ward column
wards_altered_census = wards_altered.merge(census, on='ward')
# Print the shape of wards_altered_census
print('wards_altered_census table shape:', wards_altered_census.shape)


# Merge the wards and census_altered tables on the ward column, and notice the difference in returned rows.
print(census_altered[['ward']].head())
# Merge the wards and census_altered tables on the ward column
wards_census_altered = wards.merge(census_altered,on='ward')
# Print the shape of wards_census_altered
print('wards_census_altered table shape:', wards_census_altered.shape)

#---- In step 1, the .merge() returned a table with the same number of rows as the original wards table. However, in steps 2 and 3, using the altered tables with the altered first row of the ward column, the number of returned rows was fewer. There was not a matching value in the ward column of the other table. _Remember that .merge() only returns rows where the values match in both tables._-----

# Starting with the licenses table on the left, merge it to the biz_owners table on the column account, and save the results to a variable named licenses_owners.
licenses_owners = licenses.merge(biz_owners,on='account')
# Group licenses_owners by title and count the number of accounts for each title. Save the result as counted_df
counted_df = licenses_owners.groupby('title').agg({'account':'count'})
# Sort counted_df by the number of accounts in descending order, and save this as a variable named sorted_df.
sorted_df = counted_df.sort_values(by='account',ascending=False)
# Use the .head() method to print the first few rows of the sorted_df
print(sorted_df.head())

# Merge the ridership and cal tables together, starting with the ridership table on the left and save the result to the variable ridership_cal. If you code takes too long to run, your merge conditions might be incorrect.
ridership_cal = ridership.merge(cal)

# Extend the previous merge to three tables by also merging the stations table
ridership_cal_stations = ridership.merge(cal, on=['year','month','day']) \
            				.merge(stations,on='station_id')
# Create a variable called filter_criteria to select the appropriate rows from the merged table so that you can sum the rides column
# Merge the ridership, cal, and stations tables
ridership_cal_stations = ridership.merge(cal, on=['year','month','day']) \
							.merge(stations, on='station_id')

# Create a filter to filter ridership_cal_stations
filter_criteria = ((ridership_cal_stations['month'] == 7) 
                   & (ridership_cal_stations['day_type'] =='Weekday') 
                   & (ridership_cal_stations['station_name'] == 'Wilson'))

# Use .loc and the filter to select for rides
print(ridership_cal_stations.loc[filter_criteria, 'rides'].sum())

# Starting with the licenses table, merge to it the zip_demo table on the zip column. Then merge the resulting table to the wards table on the ward column. Save result of the three merged tables to a variable named licenses_zip_ward.
licenses_zip_ward = licenses.merge(zip_demo,on='zip') \
            			.merge(wards,on='ward')
# Group the results of the three merged tables by the column alderman and find the median income.
print(licenses_zip_ward.groupby('alderman').agg({'income':'median'}))

# Merge land_use and census on the ward column. Merge the result of this with licenses on the ward column, using the suffix _cen for the left table and _lic for the right table. Save this to the variable land_cen_lic
land_cen_lic = land_use.merge(census,on='ward')\
            			.merge(licenses,on='ward',suffixes=('_cen','_lic'))

# Group land_cen_lic by ward, pop_2010 (the population in 2010), and vacant, then count the number of accounts. Save the results to pop_vac_lic.
land_cen_lic = land_use.merge(census, on='ward') \
                    .merge(licenses, on='ward', suffixes=('_cen','_lic'))
# Group by ward, pop_2010, and vacant, then count the # of accounts
pop_vac_lic = land_cen_lic.groupby(['ward', 'pop_2010','vacant']
                                   ,as_index=False).agg({'account':'count'})

# Sort pop_vac_lic by vacant, account, andpop_2010 in descending, ascending, and ascending order respectively. Save it as sorted_pop_vac_lic.
