from apache_ranger.model.ranger_service import *
from apache_ranger.client.ranger_client import *
from apache_ranger.model.ranger_policy  import *
from apache_ranger.exceptions import *
from inventory import *
import logging
import os
import datetime

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
		
	def get_role_info(self,rolename):
		try:
			x=self.conn.get_role(rolename, None, None)
			self.rootLogger.info("Role {} found".format(rolename))
			self.rootLogger.info(x)
		except RangerServiceException as e:
			self.rootLogger.error("Role {} Not found".format(rolename))
			self.rootLogger.error(e)

	def get_policy_info(self,db_name,tbl_name,service_name):
		name="db="+db_name+",tble="+tbl_name 
		print(name)
		try:
			policies = self.conn.get_policy(service_name,name)	
			print(policies)
			self.rootLogger.info("Policy {0} Found".format(name))
			self.rootLogger.info(policies)
		except ValueError as e:
			self.rootLogger.info("Policy Exception {0}".format(e))
			
			
		#for policy in policies:
		#	print('        id: ' + str(policy.id) + ', service: ' + policy.service + ', name: ' + policy.name)

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
		
	def add_policy(self,db_name,tbl_name,role_name,service_name):
		policy = RangerPolicy()
		service = RangerService()
		service.name = service_name	
		policy.name="db="+db_name+",tble="+tbl_name
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

def main():
	ranger = RangerClient(ranger_url, ranger_auth)
	ran = RangerPolicyMgm(ranger,'Dev')
	#ran.add_policy("sv_hadoop1","laksha122","sv_g_hadoop",'cm_hive')
	ran.add_policy("sv_hadoop","laksha","sv_g_hadoop",'cm_hive')
	ran.get_policy_info("sv_hadoop",'laksha','cm_hive')
	ran.get_role_info('rolename')
	#ran.create_role("rolename","sv_g_hadoop")
	#ran.list_service()

if __name__ == "__main__":
	main()
		

