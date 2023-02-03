global class GanContactCreationBatchHelper {
    global void CreateContactsOnGanAccount(map<Id,Id> parentIdToGanAccIdMap){
    	try{
    	String SobjectApiName = 'Contact';
		map<String, Schema.SObjectType> schemaMap = Schema.getGlobalDescribe();
		map<String, Schema.SObjectField> fieldMap = schemaMap.get(SobjectApiName).getDescribe().fields.getMap();
		List<String> commaSepratedFieldsList = new List<String>();
		string commaSepratedFields ='';  
	    for(String fieldName : fieldMap.keyset()){ 
    	  if(commaSepratedFields == null || commaSepratedFields == ''){
                commaSepratedFields = fieldName;
          }else{
                commaSepratedFields = commaSepratedFields + ', ' + fieldName;
          }
          commaSepratedFieldsList.add(commaSepratedFields);
    	}
	
    	List<contact> AllContactList = new List<contact>();
    	map<Id,Account> FirstLevelChildAccounts = new map<Id,Account>([select Id,parentId,Rating,Customer_Number_ID__c,EVS_ID__c,Catapult_ID__c,Domain__c,Billing_Email__c,Billing_Email_02__c,Billing_Email_03__c,Billing_Email_04__c from Account where parentId IN:parentIdToGanAccIdMap.values()]) ;
    	map<Id,Account> SecondLevelChildAccounts = new map<Id,Account>();
        map<Id,Account> ThirdLevelChildAccounts = new map<Id,Account>();
        map<Id,Account> FourthLevelChildAccounts = new map<Id,Account>();
        map<Id,List<Account>> GanAccountToChildAccountList = new map<Id,List<Account>>(); 
        map<Id,List<contact>> AccountToChildContacts = new map<Id,List<contact>>(); 
        List<contact> ClonedGanContacts = new List<Contact>();
        map<Id,Id> contactToGANAccountMap = new map<Id,Id>();
        List<Provisioning_Account__c> ProvisioningAccounts = new List<Provisioning_Account__c>();
        map<Id,List<string>> GanAccountsDomainsToBeUpdated = new map<Id,List<string>>();
        if(!FirstLevelChildAccounts.isEmpty()){
        	List<Id> FLevelAccountIds = new List<Id>();
        	FLevelAccountIds.addAll(FirstLevelChildAccounts.keyset());
        	try{
        	for(Account Acc:FirstLevelChildAccounts.values()){
        		if(Acc.Customer_Number_ID__c!=null || Acc.EVS_ID__c!=null || Acc.Catapult_ID__c!=null){
        			boolean catapultdUsed = false; 
	        			if(Acc.Customer_Number_ID__c!=null){
	        				Provisioning_Account__c provAcc = new Provisioning_Account__c(Master_Account__c=Acc.ParentId,/*Status__c='Active'*/DTH_Status__c=Acc.Rating);
	        				provAcc.DTH_ID__c = Acc.Customer_Number_ID__c;
	        				if(Acc.Catapult_ID__c!=null){
	        				provAcc.Service_Account_Number__c = Acc.Catapult_ID__c;
	        				provAcc.Platform__c = 'Catapult';
	        				catapultdUsed = true;
		        			}else{
		        				provAcc.Platform__c = 'IRIS';
		        			}
		        			ProvisioningAccounts.add(provAcc);
	        			}
	        			
	        			if(Acc.EVS_ID__c!=null){
	        				
	        				Provisioning_Account__c provAcc = new Provisioning_Account__c(Master_Account__c=Acc.ParentId,/*Status__c='Active'*/DTH_Status__c=Acc.Rating);
	        				provAcc.DTH_ID__c = Acc.EVS_ID__c;
	        				if(Acc.Catapult_ID__c!=null && !catapultdUsed){
	        				provAcc.Service_Account_Number__c = Acc.Catapult_ID__c;
	        				provAcc.Platform__c = 'Catapult';
		        			}else{
		        				provAcc.Platform__c = 'EVS';
	        				}
		        			
		        			ProvisioningAccounts.add(provAcc);
	        			}
        		}
        		if(Acc.parentId!=null){
	        		if(GanAccountsDomainsToBeUpdated.get(Acc.parentId)==null){
	        			GanAccountsDomainsToBeUpdated.put(Acc.parentId,new List<string>());        			
	        		}
	        		if(Acc.Domain__c!=null){
	        			GanAccountsDomainsToBeUpdated.get(Acc.parentId).add(Acc.Domain__c);
	        		} 
	        		
	        		if(GanAccountToChildAccountList.get(Acc.parentId)==null){
	        			GanAccountToChildAccountList.put(Acc.parentId,new List<Account>());
	        		}
	        		GanAccountToChildAccountList.get(Acc.parentId).add(Acc);
        		}
        	}
        	}
        	catch(exception e){
        		insert new Error_Log__c(trace__c = 'Error at line 79 in Contact GAN Batch'+e+'\n\n');
        	}
        	
        	string Query1 = 'select ' + commaSepratedFields + ',Account.parentId,Account.Billing_Email__c,Account.Billing_Email_02__c,Account.Billing_Email_03__c,Account.Billing_Email_04__c from contact where AccountId IN:FLevelAccountIds and Active__c = true';
        	List<Contact> FirstLevelChildContacts = Database.query(Query1);
        	
        	if(!FirstLevelChildContacts.isEmpty()){
        		AllContactList.addAll(FirstLevelChildContacts);
        		for(contact c:FirstLevelChildContacts){
        				contactToGANAccountMap.put(c.Id,c.Account.parentId);
        				if(AccountToChildContacts.get(c.AccountId)==null){
        					AccountToChildContacts.put(c.AccountId,new List<contact>());
        				}
        				AccountToChildContacts.get(c.AccountId).add(c);
        		}
        	}
        	
        	
        	
        	SecondLevelChildAccounts = new map<Id,Account>([select Id,Rating,ParentId,Parent.ParentId,Customer_Number_ID__c,EVS_ID__c,Catapult_ID__c,Domain__c,Billing_Email__c,Billing_Email_02__c,Billing_Email_03__c,Billing_Email_04__c from Account where ParentId IN:FirstLevelChildAccounts.keyset()]);
        	
        	if(!SecondLevelChildAccounts.isEmpty()){
        		List<Id> SLevelAccountIds = new List<Id>();
        		SLevelAccountIds.addAll(SecondLevelChildAccounts.keyset());
        		try{
        		for(Account Acc:SecondLevelChildAccounts.values()){
	        		if(Acc.Customer_Number_ID__c!=null || Acc.EVS_ID__c!=null || Acc.Catapult_ID__c!=null){
	        			//Provisioning_Account__c provAcc = new Provisioning_Account__c(Master_Account__c=Acc.Parent.ParentId,Status__c='Active',DTH_Status__c=Acc.Rating); 
	        			boolean catapultdUsed = false; 
	        			if(Acc.Customer_Number_ID__c!=null){
	        				Provisioning_Account__c provAcc = new Provisioning_Account__c(Master_Account__c=Acc.Parent.ParentId,/*Status__c='Active'*/DTH_Status__c=Acc.Rating);
	        				provAcc.DTH_ID__c = Acc.Customer_Number_ID__c;
	        				if(Acc.Catapult_ID__c!=null){
	        				provAcc.Service_Account_Number__c = Acc.Catapult_ID__c;
	        				provAcc.Platform__c = 'Catapult';
	        				catapultdUsed = true;
		        			}else{
		        				provAcc.Platform__c = 'IRIS';
		        			}
		        			ProvisioningAccounts.add(provAcc);
	        			}
	        			
	        			if(Acc.EVS_ID__c!=null){
	        				
	        				Provisioning_Account__c provAcc = new Provisioning_Account__c(Master_Account__c=Acc.Parent.ParentId,/*Status__c='Active'*/DTH_Status__c=Acc.Rating);
	        				provAcc.DTH_ID__c = Acc.EVS_ID__c;
	        				if(Acc.Catapult_ID__c!=null && !catapultdUsed){
	        				provAcc.Service_Account_Number__c = Acc.Catapult_ID__c;
	        				provAcc.Platform__c = 'Catapult';
		        			}else{
		        				provAcc.Platform__c = 'EVS';
	        				}
		        			
		        			ProvisioningAccounts.add(provAcc);
	        			}
	        			
	        			
	        		}
	        		if(Acc.Parent.ParentId!=null){
		        		if(GanAccountsDomainsToBeUpdated.get(Acc.Parent.ParentId)==null){
		        			GanAccountsDomainsToBeUpdated.put(Acc.Parent.ParentId,new List<string>());         			
		        		}
		        		if(Acc.Domain__c!=null){
		        				GanAccountsDomainsToBeUpdated.get(Acc.Parent.ParentId).add(Acc.Domain__c);
		        		}
		        		
		        		if(GanAccountToChildAccountList.get(Acc.Parent.ParentId)==null){
	        				GanAccountToChildAccountList.put(Acc.Parent.ParentId,new List<Account>());
	        			}
	        			GanAccountToChildAccountList.get(Acc.Parent.ParentId).add(Acc);
	        		}
	        		
	        	}
        		}catch(exception e){
        			insert new Error_Log__c(trace__c = 'Error at line 153 in GAN Contact Creation Batch'+e+'\n\n');
        		}
        		string Query2 = 'select ' + commaSepratedFields + ',Account.Parent.ParentId,Account.Billing_Email__c,Account.Billing_Email_02__c,Account.Billing_Email_03__c,Account.Billing_Email_04__c,Account.Domain__c from contact where AccountId IN:SLevelAccountIds and Active__c = true';
        		List<contact> SecondLevelChildContacts = Database.query(Query2);
        		
        		if(!SecondLevelChildContacts.isEmpty()){
        			AllContactList.addAll(SecondLevelChildContacts);
        			for(contact c:SecondLevelChildContacts){
        				contactToGANAccountMap.put(c.Id,c.Account.Parent.parentId);
        				if(AccountToChildContacts.get(c.AccountId)==null){
        					AccountToChildContacts.put(c.AccountId,new List<contact>());
        				}
        				AccountToChildContacts.get(c.AccountId).add(c);
        			}
        		}
        		
        		ThirdLevelChildAccounts = new map<Id,Account>([select Id,Rating,ParentId,Parent.ParentId,Parent.Parent.ParentId,Customer_Number_ID__c,EVS_ID__c,Catapult_ID__c,Domain__c,Billing_Email__c,Billing_Email_02__c,Billing_Email_03__c,Billing_Email_04__c from Account where ParentId IN:SecondLevelChildAccounts.keyset()]);
        		
        		if(!ThirdLevelChildAccounts.isEmpty()){
        			List<Id> TLevelAccountIds = new List<Id>();
	        		TLevelAccountIds.addAll(ThirdLevelChildAccounts.keyset());
	        		try{
	        		for(Account Acc:ThirdLevelChildAccounts.values()){
		        		if(Acc.Customer_Number_ID__c!=null || Acc.EVS_ID__c!=null || Acc.Catapult_ID__c!=null){
		        			boolean catapultdUsed = false; 
		        			if(Acc.Customer_Number_ID__c!=null){
		        				Provisioning_Account__c provAcc = new Provisioning_Account__c(Master_Account__c=Acc.Parent.Parent.ParentId,/*Status__c='Active'*/DTH_Status__c=Acc.Rating);
		        				provAcc.DTH_ID__c = Acc.Customer_Number_ID__c;
		        				if(Acc.Catapult_ID__c!=null){
		        				provAcc.Service_Account_Number__c = Acc.Catapult_ID__c;
		        				provAcc.Platform__c = 'Catapult';
		        				catapultdUsed = true;
			        			}else{
			        				provAcc.Platform__c = 'IRIS';
			        			}
			        			ProvisioningAccounts.add(provAcc);
		        			}
		        			
		        			if(Acc.EVS_ID__c!=null){
		        				
		        				Provisioning_Account__c provAcc = new Provisioning_Account__c(Master_Account__c=Acc.Parent.Parent.ParentId,/*Status__c='Active'*/DTH_Status__c=Acc.Rating);
		        				provAcc.DTH_ID__c = Acc.EVS_ID__c;
		        				if(Acc.Catapult_ID__c!=null && !catapultdUsed){
		        				provAcc.Service_Account_Number__c = Acc.Catapult_ID__c;
		        				provAcc.Platform__c = 'Catapult';
			        			}else{
			        				provAcc.Platform__c = 'EVS';
		        				}
			        			
			        			ProvisioningAccounts.add(provAcc);
		        			}
		        		}
		        		if(Acc.Parent.Parent.ParentId!=null){
			        		if(GanAccountsDomainsToBeUpdated.get(Acc.Parent.Parent.ParentId)==null){
		        				GanAccountsDomainsToBeUpdated.put(Acc.Parent.Parent.ParentId,new List<string>());         			
			        		}
			        		if(Acc.Domain__c!=null){
			        			GanAccountsDomainsToBeUpdated.get(Acc.Parent.Parent.ParentId).add(Acc.Domain__c);
			        		}
				        	if(GanAccountToChildAccountList.get(Acc.Parent.Parent.ParentId)==null){
		        				GanAccountToChildAccountList.put(Acc.Parent.Parent.ParentId,new List<Account>());
		        			}
		        			GanAccountToChildAccountList.get(Acc.Parent.Parent.ParentId).add(Acc);
		        		}
		        		
		        	}
	        		}catch(exception e){
	        			insert new Error_Log__c(trace__c = 'Error at line 220 in GAN contact Creation Batch'+e+'\n\n');
	        		}
	        		string Query3 = 'select ' + commaSepratedFields + ',Account.Parent.Parent.ParentId,Account.Billing_Email__c,Account.Billing_Email_02__c,Account.Billing_Email_03__c,Account.Billing_Email_04__c,Account.Domain__c from contact where AccountId IN:TLevelAccountIds and Active__c = true';
	        		List<contact> ThirdLevelChildContacts = Database.query(Query3);
	        		
	        		if(!ThirdLevelChildContacts.isEmpty()){
	        			AllContactList.addAll(ThirdLevelChildContacts);
	        			for(contact c:ThirdLevelChildContacts){
	        				contactToGANAccountMap.put(c.Id,c.Account.Parent.Parent.parentId);
		        			if(AccountToChildContacts.get(c.AccountId)==null){
	        					AccountToChildContacts.put(c.AccountId,new List<contact>());
	        				}
	        				AccountToChildContacts.get(c.AccountId).add(c);
	        			}
	        		}
	        		
	        		FourthLevelChildAccounts = new map<Id,Account>([select Id,Rating,ParentId,Parent.ParentId,Parent.Parent.ParentId,Parent.Parent.Parent.ParentId,Customer_Number_ID__c,EVS_ID__c,Catapult_ID__c,Domain__c,Billing_Email__c,Billing_Email_02__c,Billing_Email_03__c,Billing_Email_04__c from Account where ParentId IN:ThirdLevelChildAccounts.keyset()]);
	        		
	        		if(!FourthLevelChildAccounts.isEmpty()){
	        			List<Id> FrLevelAccountIds = new List<Id>();
	        			FrLevelAccountIds.addAll(FourthLevelChildAccounts.keyset());
	        			try{
	        			for(Account Acc:FourthLevelChildAccounts.values()){
			        		if(Acc.Customer_Number_ID__c!=null || Acc.EVS_ID__c!=null || Acc.Catapult_ID__c!=null){
			        			boolean catapultdUsed = false; 
			        			if(Acc.Customer_Number_ID__c!=null){
			        				Provisioning_Account__c provAcc = new Provisioning_Account__c(Master_Account__c=Acc.Parent.Parent.Parent.ParentId,/*Status__c='Active'*/DTH_Status__c=Acc.Rating);
			        				provAcc.DTH_ID__c = Acc.Customer_Number_ID__c;
			        				if(Acc.Catapult_ID__c!=null){
			        				provAcc.Service_Account_Number__c = Acc.Catapult_ID__c;
			        				provAcc.Platform__c = 'Catapult';
			        				catapultdUsed = true;
				        			}else{
				        				provAcc.Platform__c = 'IRIS';
				        			}
				        			ProvisioningAccounts.add(provAcc);
			        			}
			        			
			        			if(Acc.EVS_ID__c!=null){
			        				
			        				Provisioning_Account__c provAcc = new Provisioning_Account__c(Master_Account__c=Acc.Parent.Parent.Parent.ParentId,/*Status__c='Active'*/DTH_Status__c=Acc.Rating);
			        				provAcc.DTH_ID__c = Acc.EVS_ID__c;
			        				if(Acc.Catapult_ID__c!=null && !catapultdUsed){
			        				provAcc.Service_Account_Number__c = Acc.Catapult_ID__c;
			        				provAcc.Platform__c = 'Catapult';
				        			}else{
				        				provAcc.Platform__c = 'EVS';
			        				}
				        			
				        			ProvisioningAccounts.add(provAcc);
			        			}
			        		}
			        		if(Acc.Parent.Parent.Parent.ParentId!=null){
				        		if(GanAccountsDomainsToBeUpdated.get(Acc.Parent.Parent.Parent.ParentId)==null){
			        				GanAccountsDomainsToBeUpdated.put(Acc.Parent.Parent.Parent.ParentId,new List<string>());         			
				        		}
				        		if(Acc.Domain__c!=null){
				        			GanAccountsDomainsToBeUpdated.get(Acc.Parent.Parent.Parent.ParentId).add(Acc.Domain__c);
				        		}
				        		if(GanAccountToChildAccountList.get(Acc.Parent.Parent.Parent.ParentId)==null){
		        					GanAccountToChildAccountList.put(Acc.Parent.Parent.Parent.ParentId,new List<Account>());
			        			}
			        			GanAccountToChildAccountList.get(Acc.Parent.Parent.Parent.ParentId).add(Acc);
			        		}
			        		
			        	}
	        			}catch(exception e){
	        				insert new Error_Log__c(trace__c = 'Error at Line 287'+e+'\n\n');
	        			}
		        		string Query4 = 'select ' + commaSepratedFields + ',Account.Parent.Parent.Parent.ParentId,Account.Billing_Email__c,Account.Billing_Email_02__c,Account.Billing_Email_03__c,Account.Billing_Email_04__c,Account.Domain__c from contact where AccountId IN:FrLevelAccountIds and Active__c = true';
		        		List<contact> FourthLevelChildContacts = Database.query(Query4);
		        		
		        		if(!FourthLevelChildContacts.isEmpty()){
		        			AllContactList.addAll(FourthLevelChildContacts);
		        			for(contact c:FourthLevelChildContacts){
	        					contactToGANAccountMap.put(c.Id,c.Account.Parent.Parent.Parent.parentId);
	        					if(AccountToChildContacts.get(c.AccountId)==null){
		        					AccountToChildContacts.put(c.AccountId,new List<contact>());
		        				}
		        				AccountToChildContacts.get(c.AccountId).add(c);
	        				}
		        		}
	        		}
        		}
        	}	
        }
        
        if(!AllContactList.isEmpty()){
        	boolean billingContactExists = false; 
        	try{
        	for(contact c:AllContactList){
        		c.Active__c = false;        		
        		contact con = c.clone(false, true, false, true);
        		if(con.Account.Billing_Email__c!=null ||con.Account.Billing_Email_02__c!=null || con.Account.Billing_Email_03__c!=null || con.Account.Billing_Email_04__c!=null) {
        			if(con.Email == con.Account.Billing_Email__c || con.Email == con.Account.Billing_Email_02__c || con.Email == con.Account.Billing_Email_03__c || con.Email == con.Account.Billing_Email_04__c){
	        			if(con.Contact_Role__c!=null && !con.Contact_Role__c.contains('Billing')){
	        				con.Contact_Role__c = con.Contact_Role__c+';'+ 'Billing';
	        			}
        			}       			        			 
        		}
        		con.Active__c=true;
        		con.Jigsaw = null;
        		if(contactToGANAccountMap.get(c.Id)!=null){
	        		Id GanAccId = contactToGANAccountMap.get(c.Id);
	        		con.AccountId = GanAccId;
	        		ClonedGanContacts.add(con);
        		}
        	}
        	}catch(exception e){
        		insert new Error_Log__c(trace__c = 'Error at line 329 in GAN contact Creation'+e+'\n\n');
        	}
        	try{
	        	if(!ClonedGanContacts.isEmpty()){
	        		insert ClonedGanContacts;
	        		
	        		update AllContactList;
	        	}
        	}
        	
        	catch(exception e){
				insert new Error_Log__c(trace__c = 'Error When Inserting Cloned Gan Contacts'+e+'\n\n');
			}
        }
        try{
	        if(!ProvisioningAccounts.isEmpty()){
	        	insert ProvisioningAccounts;
	        }
        }
        catch(exception e){
        	insert new Error_Log__c(trace__c = 'Error When Inserting Provisioning Accounts'+e+'\n\n');
        }
        if(!GanAccountsDomainsToBeUpdated.isEmpty()){
        	List<Account> GansToBeUpdated = new List<Account>();
        	for(Id GanId:GanAccountsDomainsToBeUpdated.keyset()){
        		List<string> domains = GanAccountsDomainsToBeUpdated.get(GanId); 
        		if(!domains.isEmpty()){
        			String s = String.join(domains, ' ');
	        		List<String> DomainsList = s.split(' ');
	        		set<string> domainset = new set<string>();
	        		for(string str:DomainsList){
	        			if(!domainset.contains(str.toLowerCase())){
	        				domainset.add(str);
	        			}
	        		}
	        		List<string> DomainListFromSet = new List<string>();
	        		DomainListFromSet.addAll(domainset);
	        		String FinalDomains = string.join(DomainListFromSet,' ');
	        		Account Acc = new Account(Id=GanId,Domain__c=FinalDomains);
	        		GansToBeUpdated.add(Acc);
        		}
        	}
        	try{
	        	if(!GansToBeUpdated.isEmpty()){
	        		update GansToBeUpdated;
	        	}
        	}
        	catch(exception e){
        		insert new Error_Log__c(trace__c = 'Error When Updating Domains on GAN'+e+'\n\n');
        	}
        }
        if(!GanAccountToChildAccountList.isEmpty() && !AccountToChildContacts.isEmpty()){
        	List<contact> BillingContactsToBeInserted = new List<contact>();
        	try{
        	for(Id GanId:GanAccountToChildAccountList.keyset()){
        		system.debug('test at 384 '+GanAccountToChildAccountList);
        		if(!GanAccountToChildAccountList.get(GanId).isEmpty()){
        			for(Account acc:GanAccountToChildAccountList.get(GanId)){
        				boolean billingContactDoesNotExist = false; 
        				try{
	        				system.debug('at 389 '+AccountToChildContacts.get(acc.Id));
	        				if(!AccountToChildContacts.isEmpty() && AccountToChildContacts.get(acc.Id)!=null){      					
	        					for(Contact con:AccountToChildContacts.get(acc.Id)){
	        						system.debug('at 392 '+con); 
	        						system.debug('at 393 '+acc);
	        						system.debug('at 394 '+con.Email); 
	        						system.debug('at 395 '+acc.Billing_Email__c); 
	        						system.debug('at 396 '+acc.Billing_Email_02__c); 
	        						system.debug('at 397 '+acc.Billing_Email_03__c); 
	        						system.debug('at 398 '+acc.Billing_Email_04__c); 
	        						if((con.Email!=acc.Billing_Email__c) && (con.Email!=acc.Billing_Email_02__c) && (con.Email!=acc.Billing_Email_03__c) && (con.Email!=acc.Billing_Email_04__c)){
	        							billingContactDoesNotExist=true;
	        						}	        						
	        					}
	        				}else{
	        					billingContactDoesNotExist=true;
	        				}
        				}catch(exception e){
        					insert new Error_Log__c(trace__c = 'Error at 319 on Billing Emails Section in GAN Contact Creation Batch'+e+'\n\n');
        				}
        				try{
        				system.debug('at 405 '+billingContactDoesNotExist);
        				if(billingContactDoesNotExist){
        					if(acc.Billing_Email__c!=null){
        						Contact con = new Contact(FirstName='Billing',LastName='Contact',Email=acc.Billing_Email__c,contact_role__c='Billing COntact',AccountId=GanId);
        						BillingContactsToBeInserted.add(con);
        					}
        					if(acc.Billing_Email_02__c!=null){
        						Contact con = new Contact(FirstName='Billing',LastName='Contact',Email=acc.Billing_Email_02__c,contact_role__c='Billing COntact',AccountId=GanId);
        						BillingContactsToBeInserted.add(con);
        					}
        					if(acc.Billing_Email_03__c!=null){
        						Contact con = new Contact(FirstName='Billing',LastName='Contact',Email=acc.Billing_Email_03__c,contact_role__c='Billing COntact',AccountId=GanId);
        						BillingContactsToBeInserted.add(con);
        					}
        					if(acc.Billing_Email_04__c!=null){
        						Contact con = new Contact(FirstName='Billing',LastName='Contact',Email=acc.Billing_Email_04__c,contact_role__c='Billing COntact',AccountId=GanId);
        						BillingContactsToBeInserted.add(con);
        					}
        					
        				}
        				}catch(exception e){
        					insert new Error_Log__c(trace__c = 'Error at 419 on Billing Emails Section in GAN Contact Creation Batch'+e+'\n\n');
        				}
        			}
        		}
        	}
        	}
        	catch(exception e){
        		insert new Error_Log__c(trace__c = 'Error at 426 on Billing Emails Section in GAN Contact Creation Batch'+e+'\n\n');
        	}
        	try{
	        	if(!BillingContactsToBeInserted.isEmpty()){
	        		insert BillingContactsToBeInserted;
	        	}
        	}catch(exception e){
        		insert new Error_Log__c(trace__c = 'Error When Inserting Billing Contacts'+e+'\n\n');
        	}
        }
    	}
    	catch(exception e){
    		insert new Error_Log__c(trace__c = 'Error in GAN Contact Creation Batch'+e+'\n\n');
    	}
    }
}
