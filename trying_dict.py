


dict_cols = {
	'surname': {
		'colname': 'last_name'
		, 'filename': 'secondnames.csv'
		, 'colnum' : 0
		}
	, 'name': {
		'colname': 'last_name'
		, 'filename': 'names.csv'
		, 'colnum' : 0
		}
	, 'phone': {
		'colname': 'phone'
		, 'filename': 'phones.csv'
		, 'colnum' : 0
		}
	, 'username': {
		'colname': 'username'
		, 'filename': 'username.csv'
		, 'colnum' : 0
		}
};

print('name colnum', dict_cols['name']['colnum']);

for el in dict_cols:
	print(el);

print();

for el in dict_cols:
	print(dict_cols[el]);

print();

for el in dict_cols:
	print(el);
	for k in dict_cols[el]:
		print('\t', k, '\t', dict_cols[el][k]);
	
	