
import df_val_set as dvs

print('искать ');
inp = input('введите: ');
print (inp);


dfnam = dvs.Df_val_cnt(PY_DIR+'/'+'names.csv');
dfsur = dvs.Df_val_cnt(PY_DIR+'/'+'secondnames.csv', 'secondname');
dfmail = dvs.Df_val_cnt(PY_DIR+'/'+'emails.csv');
dfphon = dvs.Df_val_cnt(PY_DIR+'/'+'phones.csv');
dfnick = dvs.Df_val_cnt(PY_DIR+'/'+'nicks.csv');
dffio = dvs.Df_fio(PY_DIR+'/'+'fio.csv');
lnk_fio_phone = dvs.Df_link(PY_DIR+'/'+'fio_phones.csv');
lnk_fio_nick = dvs.Df_link(PY_DIR+'/'+'fio_nick.csv');


# найти фио людей,у которых фамилия,имя или отчество совпадает с lf_string
def search_fio(lf_string):
	if dfnam.inlist(lf_string):
		nam_ind = dfnam.get_index(lf_string);
	if dfsur.inlist(lf_string):
		sur_ind = dfsur.get_index(lf_string);
		

