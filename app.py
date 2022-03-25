from flask import Flask, render_template, request, url_for, flash, redirect
from flask import Flask, current_app, g, render_template, request
from flask_paginate import Pagination, get_page_parameter,get_page_args
import pwd,grp
#from ranger_Policymanagment.py import RangerPolicyMgm 
from ranger_Policymanagment import CallRanger 

app = Flask(__name__)
app.config['SECRET_KEY'] = '1234567890'

@app.route('/api/get_policy_name',methods=('GET', 'POST'))

def get_policy_info():
	db_name = request.args.get('db_name')
	tbl_name = request.args.get('db_name')

@app.route('/')
def index():
		
	return render_template('index.html')

@app.route('/create')
def create():
	unix_name = request.args.get('uname')
	is_uname_exit =  False
	groups = []
	if unix_name is not None:
		try:
			pwd.getpwnam(unix_name)
			is_uname_exit = True
			groups = [g.gr_name for g in grp.getgrall() if unix_name in g.gr_mem]
			gid = pwd.getpwnam(unix_name).pw_gid
			groups.append(grp.getgrgid(gid).gr_name)	
		except KeyError:
			flash('User {} does not exist.'.format(unix_name))
			print('User {} does not exist.'.format(unix_name))	
	print(unix_name)
	print(groups)
	'''
	if not is_uname_exit:
		flash('User {} does not exist.'.format(unix_name))
	'''
	return render_template('search_unix.html',uname=unix_name,groups=groups)

def get_policy_info(env,dbname,table):
	CallRanger(env,dbname,table)

@app.route('/searchpolicy')
def searchpolicy():
	tables = request.args.get('tables')
	env = request.args.get('env')
	checkpolicy = True 
	tbl_list = []
	policy_dict = {}
	tmp = {}
	dev_arr = []
	qa_arr = []
	prod_arr = []
	all_table_list = []
	if tables is not None:
			lines=tables.split('\n')
			for x in lines:
				y = x.strip()
				db_table = y.split(".")
				if any(db_table): 
					if len(db_table) != 2:
						flash(" Invalide table name: {}".format(y))
						checkpolicy = False
						#break
					else:
						tbl_list.append(y)
						#CallRanger(env,db_table[0],db_table[1])	
			if checkpolicy:
				print(tbl_list)
				for tbl in tbl_list:
					tbl_arr = tbl.split('.')
					p_name = 'db={0},tbl={1}'.format(tbl_arr[0],tbl_arr[1])
					db_name = tbl_arr[0]
					tble_name = tbl_arr[1]
					print(p_name)
					if env == 'all':
						dev = CallRanger('dev',db_name,tble_name)
						#dev_arr.append(dev) 
						all_table_list.append(dev)
						qa = CallRanger('qa',db_name,tble_name)
						all_table_list.append(qa)
						#qa_arr.append(qa)
						#dev_arr.append(qa)
						prod = CallRanger('prod',db_name,tble_name)
						all_table_list.append(prod)
						#prod_arr.append(prod)
						#dev_arr.append(prod)
					elif env == 'dev':
						dev = CallRanger('dev',db_name,tble_name)
						all_table_list.append(dev)
					elif env == 'qa':
						qa = CallRanger('qa',db_name,tble_name) 
						all_table_list.append(qa)
					elif env == 'prod':
						prod = CallRanger('prod',db_name,tble_name)
						all_table_list.append(prod)
					else:
						print("invalid environment")
	print("----------- sasa -----------------------------")
	#print(all_table_list)
	for x in all_table_list:
		print(x)
		print(x['policyItems'])
	print("----------------------------------------")
	return render_template('search_policies.html',policy_dict1=all_table_list)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000,debug=True)
