# Use merge_ordered() to merge gdp and sp500, interpolate missing value
gdp_sp500 = pd.merge_ordered(gdp, sp500, left_on='year', right_on='date', 
                             how='left',  fill_method='ffill')
# Subset the gdp and returns columns
gdp_returns = gdp_sp500[['gdp','returns']]
# Print gdp_returns correlation
print (gdp_returns.corr())

# Use merge_ordered() to merge the inflation and unemployment tables on date with an inner join, and save the results as inflation_unemploy.
inflation_unemploy = pd.merge_ordered(inflation, unemployment, on='date', how='inner')
# Print the inflation_unemploy variable.
print(inflation_unemploy)
# Using inflation_unemploy, create a scatter plot with unemployment_rate on the horizontal axis and cpi (inflation) on the vertical axis
inflation_unemploy.plot(kind='scatter' ,x='unemployment_rate',y='cpi')
plt.show()

# Use merge_ordered() on gdp and pop, merging on columns date and country with the fill feature, save to ctry_date
ctry_date = pd.merge_ordered(gdp,pop,on=['date','country'],
                             fill_method='ffill')
print(ctry_date)
# Perform the same merge of gdp and pop, but join on country and date (reverse of step 1) with the fill feature, saving this as date_ctr
date_ctry = pd.merge_ordered(gdp,pop,on=['country','date'],fill_method='ffill')
print(date_ctry)

# Use merge_asof() to merge jpm (left table) and wells together on the date_time column, where the rows with the nearest times are matched, and with suffixes=('', '_wells'). Save to jpm_wells.
jpm_wells = pd.merge_asof(jpm, wells, on='date_time', direction='nearest', suffixes=('', '_wells'))
# Use merge_asof() to merge jpm_wells (left table) and bac together on the date_time column, where the rows with the closest times are matched, and with suffixes=('_jpm', '_bac'). Save to jpm_wells_bac.
jpm_wells_bac = pd.merge_asof(jpm_wells,bac,on='date_time',direction='nearest',suffixes=('_jpm','_bac'))
# Using price_diffs, create a line plot of the close price of JPM, WFC, and BAC only.
price_diffs = jpm_wells_bac.diff()
# Plot the price diff of the close of jpm, wells and bac only
price_diffs.plot(y=['close_jpm', 'close_wells', 'close_bac'])
plt.show()

# Using merge_asof(), merge gdp and recession on date, with gdp as the left table. Save to the variable gdp_recession.
gdp_recession = pd.merge_asof(gdp,recession,on='date')
# Create a list using a list comprehension and a conditional expression, named is_recession, where for each row if the gdp_recession['econ_status'] value is equal to 'recession' then enter 'r' else 'g'.
is_recession = ['r' if s=='recession' else 'g' for s in gdp_recession['econ_status']]
# Using gdp_recession, plot a bar chart of gdp versus date, setting the color argument equal to is_recession
gdp_recession.plot(kind='bar', y='gdp', x='date', color=is_recession, rot=90)
plt.show()

# Use merge_ordered() on gdp and pop on columns country and date with the fill feature, save to gdp_pop and print.
gdp_pop = pd.merge_ordered(gdp,pop,on=['country','date'],            fill_method='ffill')
print(gdp_pop)
# Add a column named gdp_per_capita to gdp_pop that divides gdp by pop
gdp_pop['gdp_per_capita']=gdp_pop['gdp']/gdp_pop['pop']
# Pivot gdp_pop so values='gdp_per_capita', index='date', and columns='country', save as gdp_pivot.
gdp_pivot = gdp_pop.pivot_table(values='gdp_per_capita', index='date', columns='country')
# Use .query() to select rows from gdp_pivot where date is greater than equal to "1991-01-01". Save as recent_gdp_pop.
recent_gdp_pop = gdp_pivot.query('date >= "1991-01-01"')

# Use .melt() to unpivot all of the columns of ur_wide except year and ensure that the columns with the months and values are named month and unempl_rate, respectively. Save the result as ur_tall.
ur_tall = ur_wide.melt(id_vars=['year'],var_name='month',value_name='unempl_rate')
# Add a column to ur_tall named date which combines the year and month columns as year-month format into a larger string, and converts it to a date data type.
ur_tall['date'] = pd.to_datetime(ur_tall['month'] + '-' + ur_tall['year'])
# Sort ur_tall by date and save as ur_sorted.
ur_sorted = ur_tall.sort_values('date',ascending=True)
# Using ur_sorted, plot unempl_rate on the y-axis and date on the x-axis.
ur_sorted.plot(kind='bar',y='unempl_rate',x='date')
plt.show()
# the plot shows a steady decrease in the unemployment rate with an increase near the end.

# Use .melt() on ten_yr to unpivot everything except the metric column, setting var_name='date' and value_name='close'. Save the result to bond_perc.
bond_perc = ten_yr.melt(id_vars='metric',var_name='date',value_name='close')
# Using the .query() method, select only those rows were metric equals 'close', and save to bond_perc_close.
bond_perc_close = bond_perc.query('metric == "close"')
# Use merge_ordered() to merge dji (left table) and bond_perc_close on date with an inner join, and set suffixes equal to ('_dow', '_bond'). Save the result to dow_bond.
dow_bond = pd.merge_ordered(dji,bond_perc_close,on='date',suffixes=('_dow', '_bond'),how='inner')
# Using dow_bond, plot only the Dow and bond values.
dow_bond.plot(y=['close_dow', 'close_bond'], x='date', rot=90)
plt.show() 
# The plot confirms that the bond and stock prices are inversely correlated. Often as the price of stocks increases, the price for bonds decreases.

