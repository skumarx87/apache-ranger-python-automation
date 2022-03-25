from apache_ranger.model.ranger_service import *
from apache_ranger.client.ranger_client import *
from apache_ranger.model.ranger_policy  import *
from apache_ranger.exceptions import *
from RangerClientHttp import *
from inventory import *
import logging
import os
import datetime
import argparse

class RangerPolicyMgm:
	def __init__(self,conn,env):
		date = datetime.datetime.today().strftime('%m-%d-%Y')
		log_dir = os.path.dirname(__file__)+"/logs"
		log_file="rangerPolicyadmin.log_"+env+"_"+date
		print(log_dir)
		if not os.path.exists(log_dir):
			os.makedirs(log_dir)
		self.conn = conn
		self.conn.session.verify = False

		self.rootLogger = logging.getLogger(env)
		self.logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
		#self.logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s] [%(name)s] %(message)s")

		self.fileHandler = logging.FileHandler("{0}/{1}".format(log_dir,log_file))
		self.fileHandler.setFormatter(self.logFormatter)
		self.rootLogger.addHandler(self.fileHandler)
		
		self.consoleHandler = logging.StreamHandler()
		self.consoleHandler.setFormatter(self.logFormatter)
		self.rootLogger.addHandler(self.consoleHandler)
		self.rootLogger.setLevel(logging.INFO)

	def get_group_info(self,groupName,url,auth):
		r_c_h = RangerClientHttp(url,auth)
		r_c_h.session.verify = False
		URI_GET_GROUP='/service/xusers/groups/groupName/{name}'
		GET_GROUP=API(URI_GET_GROUP,HttpMethod.GET, HTTPStatus.OK)

		try:
			x=r_c_h.call_api(GET_GROUP.format_path({ 'name': groupName }))
			self.rootLogger.info("Group {0} found and id is {1}".format(x['name'],x['id']))
		except AttributeError as e:
			self.rootLogger.error("Group {} Not found".format(groupName))
			self.rootLogger.error(e)
		
		
	def get_role_info(self,rolename):
		try:
			x=self.conn.get_role(rolename, None, None)
			self.rootLogger.info("Role {} found".format(rolename))
			self.rootLogger.info(x)
		except RangerServiceException as e:
			self.rootLogger.error("Role {} Not found".format(rolename))
			self.rootLogger.error(e)

	def get_policy_info(self,db_name,tbl_name,service_name):
		return_v = None
		name="db="+db_name+",tble="+tbl_name 
		print(name)
		try:
			policies = self.conn.get_policy(service_name,name)	
			print(policies)
			self.rootLogger.info("Policy {0} Found".format(name))
			#self.rootLogger.info(policies)
			return_v = policies 
		except ValueError as e:
			self.rootLogger.info("Policy Exception {0}".format(e))
			return_v = None
			
		return return_v

	def list_service(self):
		retrieved_service = self.conn.get_service('hive')
		service_defs = self.conn.find_service_defs()
		for service_def in service_defs:
			print('        ' + 'id: ' + str(service_def.id) + ', name: ' + service_def.name+', type: ' + service_def.type)
	def create_role(self,role_name,group_name):
		role1 = RangerRole()
		role1.groups = [{"name":group_name,"isAdmin":"false"}]
		role1.name=role_name
		try:
			self.conn.create_role(None,role1)
			self.rootLogger.info("Role {} has been Created".format(role_name))
		except RangerServiceException as e:
			self.rootLogger.error("Error while creating Role")
			self.rootLogger.error(e)
	def update_policy(self,db_name,tbl_name,role_name,service_name):
		print("Received rolename is {}".format(role_name))
		pname="db="+db_name+",tble="+tbl_name
		policy_info=self.get_policy_info(db_name,tbl_name,service_name)
		if policy_info is None:
			self.rootLogger.error("Policy Name {} doesn't exit".format(pname))
			return None
		allowItem1          = RangerPolicyItem()
		allowItem1.roles = [role_name ]
		allowItem1.accesses = [ RangerPolicyItemAccess({ 'type': 'select' }),
					RangerPolicyItemAccess({ 'type': 'update' }) ]
		policy_info.policyItems.append(allowItem1)
		
		try:
			self.rootLogger.info('Updating policy: name=' + pname)
			updateded_policy = self.conn.update_policy(service_name,pname,policy_info)
			self.rootLogger.info('Updated policy: name=' + updateded_policy.name + ', id=' + str(updateded_policy.id))
		except RangerServiceException as e:
			self.rootLogger.error("Error while Updating Policy")
			self.rootLogger.error(e)
	def add_policy(self,db_name,tbl_name,role_name,service_name):
		pname="db="+db_name+",tble="+tbl_name
		if self.get_policy_info(db_name,tbl_name,service_name) is not None:
			self.rootLogger.error("Policy Name {} already exit".format(pname))
			return None
			 
		policy = RangerPolicy()
		policy.name=pname
		service = RangerService()
		service.name = service_name	
		policy.service = service.name
		policy.resources = {'database': RangerPolicyResource({ 'values': [db_name]}),
				'table':    RangerPolicyResource({ 'values': [tbl_name] }),
				'column':   RangerPolicyResource({ 'values': ['*'] }) }

		allowItem1          = RangerPolicyItem()
		allowItem1.roles = [role_name ]
		allowItem1.accesses = [ RangerPolicyItemAccess({ 'type': 'select' }),
					RangerPolicyItemAccess({ 'type': 'refresh' }) ]
		policy.policyItems     = [ allowItem1 ]
		try:
			self.rootLogger.info('Creating policy: name=' + policy.name)
			created_policy = self.conn.create_policy(policy)
			self.rootLogger.info('created policy: name=' + created_policy.name + ', id=' + str(created_policy.id))
		except RangerServiceException as e:
			self.rootLogger.error("Error while creating Policy")
			self.rootLogger.error(e)
def CallRanger(env,dbname,table):
	print("inside ranger code")
	if env == 'dev':
		ranger = RangerClient(ranger_url, ranger_auth)
	elif env == 'qa':
		ranger = RangerClient(ranger_url, ranger_auth)
	elif env == 'prod':
		ranger = RangerClient(ranger_url, ranger_auth)
	ran = RangerPolicyMgm(ranger,env)
	obj = ran.get_policy_info(dbname,table,'cm_hive')
	tbl = "db={},tbl={}(Not_found)".format(dbname,table)
	
	if obj is not None:
		obj['env'] = env
		return obj
	else:
		obj = {'name':tbl,'env' : env,"policyItems": [{"accesses": [{"type": "NA"}], "roles": ["NA"]}]}
		return obj

def main():
	
	parser = argparse.ArgumentParser(description='Optional app description')
	parser.add_argument('env',help="environment name")
	parser.add_argument('dbname',help='db name')
	parser.add_argument('table',help='table name')
	args = parser.parse_args()
	if args.env == 'dev':
		ranger = RangerClient(ranger_url, ranger_auth)
	elif args.env == 'qa':
		ranger = RangerClient(ranger_url, ranger_auth)
	elif args.env == 'prod':
		ranger = RangerClient(ranger_url, ranger_auth)
	
	ran = RangerPolicyMgm(ranger,args.env)
	ran.get_policy_info(args.dbname,args.table,'cm_hive')
	#ran.add_policy("sv_hadoop1","laksha122","sv_g_hadoop",'cm_hive')
	#ran.add_policy("sv_hadoop","laksha","sv_g_hadoop",'cm_hive')
	ran.update_policy("sv_hadoop","laksha","kudu",'cm_hive')
	#ran.get_policy_info("sv_hadoop",'laksha','cm_hive')
	ran.get_group_info("sv__hadoop",ranger_url,ranger_auth)
	#ss.get_role('rolename')
	#ran.create_role("rolename","sv_g_hadoop")
	#ran.list_service()

if __name__ == "__main__":
	main()
		

