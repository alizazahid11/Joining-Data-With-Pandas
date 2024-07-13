# Merge employees and top_cust
empl_cust = employees.merge(top_cust, on='srid', 
                                 how='left', indicator=True)
# Select the srid column where _merge is left_only
srid_list = empl_cust.loc[empl_cust['_merge'] == 'left_only', 'srid']
# Get employees not working with top customers
print(employees[employees['srid'].isin(srid_list)])

# Merge non_mus_tcks and top_invoices on tid using an inner join. Save the result as tracks_invoices.
tracks_invoices = non_mus_tcks.merge(top_invoices,on='tid',how='inner')
# Use .isin() to subset the rows of non_mus_tck where tid is in the tid column of tracks_invoices. Save the result as top_tracks.
top_tracks = non_mus_tcks[non_mus_tcks['tid'].isin(tracks_invoices['tid'])]
# Group top_tracks by gid and count the tid rows. Save the result to cnt_by_gid.
cnt_by_gid = top_tracks.groupby(['gid'], as_index=False).agg({'tid':'count'})
# Merge cnt_by_gid with the genres table on gid and print the result.
print(cnt_by_gid.merge(genres,on='gid'))

# Concatenate tracks_master, tracks_ride, and tracks_st, in that order, setting sort to True
tracks_from_albums = pd.concat([tracks_master,tracks_ride,tracks_st],
                               sort=True)
print(tracks_from_albums)
# Concatenate tracks_master, tracks_ride, and tracks_st, where the index goes from 0 to n-1.
tracks_from_albums = pd.concat([tracks_master,tracks_ride,tracks_st],
                               ignore_index=True,
                               sort=True)
print(tracks_from_albums)
# Concatenate tracks_master, tracks_ride, and tracks_st, showing only columns that are in all tables.
tracks_from_albums = pd.concat([tracks_master,tracks_ride,tracks_st],
                               join='inner',
                               sort=True)
print(tracks_from_albums)

# Concatenate the three tables together vertically in order with the oldest month first, adding '7Jul', '8Aug', and '9Sep' as keys for their respective months, and save to variable avg_inv_by_month.
inv_jul_thr_sep = pd.concat([inv_jul, inv_aug, inv_sep], 
                            keys=['7Jul', '8Aug', '9Sep'])
# Use the .agg() method to find the average of the total column from the grouped invoices.
avg_inv_by_month = inv_jul_thr_sep.groupby(level=0).agg({'total':'mean'})
# Create a bar chart of avg_inv_by_month.
avg_inv_by_month.plot(kind='bar')
plt.show()

# Concatenate the classic_18 and classic_19 tables vertically where the index goes from 0 to n-1, and save to classic_18_19.
classic_18_19 = pd.concat([classic_18,classic_19],ignore_index=True)
# Concatenate the pop_18 and pop_19 tables vertically where the index goes from 0 to n-1, and save to pop_18_19.
pop_18_19 = pd.concat([pop_18,pop_19],axis=0,ignore_index=True)
# Merge classic_18_19 with pop_18_19
classic_pop = classic_18_19.merge(pop_18_19,on='tid')
# Using .isin(), filter classic_18_19 rows where tid is in classic_pop
popular_classic = classic_18_19[classic_18_19['tid'].isin(classic_pop['tid'])]
# Print popular chart
print(popular_classic)