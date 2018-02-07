package com.ml.usermapping.domain;

import java.util.HashMap;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;

public class BHCEPMapping {
	
	/**
	 * Columns in DynamoDB table: apikey-cep-mapping
	 */
	public static enum FIELDS {
		// API Key as Primary Key
		BHAPIKey("BHAPIKey"),
		// Password of API key, Shared secert
		BHSharedSecret("BHSharedSecret"),
		// Password of CEP API user
		CEPPassword("CEPPassword"),
		// CEP API User
		CEPUser("CEPUser"),
		// BH Client Id
		Client_Id("Client_Id"),
		// BH Client code hash
		CodeHash("CodeHash"),
		// BH IP whitelist
		IP_Whitelist("IP_Whitelist"),
		// BH Partner allowed methods
		Partner_Allowed_Methods("Partner_Allowed_Methods"),
		// BH partner id
		Partner_Id("Partner_Id"),
		// BH Reseller Id
		Reseller_Id("Reseller_Id"),
		// BH DB Schema
		Schema("Schema");
		
		private String fieldName;
		
		FIELDS(String fieldName) {
			this.fieldName = fieldName;
		}
		
		public String getField() {
			return fieldName;
		}
		
		@Override
		public String toString() {
			return getField();
		}
	}
	
	public static HashMap<String, FIELDS> ToFields = new HashMap<String, FIELDS>();
	static {
	    ToFields.put("Client_Id", FIELDS.Client_Id);
	    ToFields.put("BHAPIKey", FIELDS.BHAPIKey);
	    ToFields.put("Partner_Allowed_Methods", FIELDS.Partner_Allowed_Methods);
	    ToFields.put("Reseller_Id", FIELDS.Reseller_Id);
	    ToFields.put("Schema", FIELDS.Schema);
	    ToFields.put("BHSharedSecret", FIELDS.BHSharedSecret);
	    ToFields.put("CEPUser", FIELDS.CEPUser);
	    ToFields.put("CEPPassword", FIELDS.CEPPassword);
	    ToFields.put("CodeHash", FIELDS.CodeHash);
	    ToFields.put("Partner_Id", FIELDS.Partner_Id);
	    ToFields.put("IP_Whitelist", FIELDS.IP_Whitelist);
	}
		
	public void set(FIELDS f, String val) {
		switch(f) {
		case Client_Id:
			this.clientId = val;
			break;
		case BHAPIKey:
			this.bHApiKey = val;
			break;
		case Partner_Allowed_Methods:
			this.partnerAllowedMethods = val;
			break;
		case Reseller_Id:
			this.resellerId = val;
			break;
		case Schema:
			this.schema = val;
			break;
		case BHSharedSecret:
			this.bHSharedSecret = val;
			break;
		case CEPUser:
			this.cEPUser = val;
			break;
		case CEPPassword:
			this.cEPPassword = val;
			break;
		case CodeHash:
			this.codeHash = val;
			break;
		case Partner_Id:
			this.partnerId = val;
			break;
		case IP_Whitelist:
			this.iPWhiteList = val;
			break;
		}
						
	}
		
	/**
	 * Member variables 
	 */
	private String bHApiKey;
	private String bHSharedSecret;
	private String cEPPassword;
	private String cEPUser;
	private String clientId;
	private String codeHash;
	private String iPWhiteList;
	private String partnerAllowedMethods;
	private String partnerId;
	private String resellerId;
	private String schema;
	
	// End of member variables
	
	/**
	 * Auto-generated setter and getter
	 * Don't change it, Jackson use setter/getter to auto-convert json doc to object 
	 */
	
	public String getbHApiKey() {
		return bHApiKey;
	}

	public void setbHApiKey(String bHApiKey) {
		this.bHApiKey = bHApiKey;
	}

	public String getbHSharedSecret() {
		return bHSharedSecret;
	}

	public void setbHSharedSecret(String bHSharedSecret) {
		this.bHSharedSecret = bHSharedSecret;
	}

	public String getcEPPassword() {
		return cEPPassword;
	}

	public void setcEPPassword(String cEPPassword) {
		this.cEPPassword = cEPPassword;
	}

	public String getcEPUser() {
		return cEPUser;
	}

	public void setcEPUser(String cEPUser) {
		this.cEPUser = cEPUser;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getCodeHash() {
		return codeHash;
	}

	public void setCodeHash(String codeHash) {
		this.codeHash = codeHash;
	}

	public String getiPWhiteList() {
		return iPWhiteList;
	}

	public void setiPWhiteList(String iPWhiteList) {
		this.iPWhiteList = iPWhiteList;
	}

	public String getPartnerAllowedMethods() {
		return partnerAllowedMethods;
	}

	public void setPartnerAllowedMethods(String partnerAllowedMethods) {
		this.partnerAllowedMethods = partnerAllowedMethods;
	}

	public String getPartnerId() {
		return partnerId;
	}

	public void setPartnerId(String partnerId) {
		this.partnerId = partnerId;
	}

	public String getResellerId() {
		return resellerId;
	}

	public void setResellerId(String resellerId) {
		this.resellerId = resellerId;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}
	// End of auto-generated setter/getter

	/**
	 * 
	 * @return a hashMap ready for dynamoDB add
	 */
	public HashMap<String, AttributeValue> toMapForAdd() {
		HashMap<String, AttributeValue> item_values = new HashMap<String, AttributeValue>();
		item_values.put(FIELDS.BHAPIKey.fieldName, new AttributeValue(bHApiKey));
		item_values.put(FIELDS.BHSharedSecret.fieldName, new AttributeValue(bHSharedSecret));
		item_values.put(FIELDS.Client_Id.fieldName, new AttributeValue(clientId));
		item_values.put(FIELDS.CodeHash.fieldName, new AttributeValue(codeHash));
		item_values.put(FIELDS.IP_Whitelist.fieldName, new AttributeValue(iPWhiteList));
		item_values.put(FIELDS.Partner_Allowed_Methods.fieldName, new AttributeValue(partnerAllowedMethods));
		item_values.put(FIELDS.Partner_Id.fieldName, new AttributeValue(partnerId));
		item_values.put(FIELDS.Reseller_Id.fieldName, new AttributeValue(resellerId));
		item_values.put(FIELDS.Schema.fieldName, new AttributeValue(schema));
		item_values.put(FIELDS.CEPUser.fieldName, new AttributeValue(cEPUser));
		item_values.put(FIELDS.CEPPassword.fieldName, new AttributeValue(cEPPassword));
		return item_values;
	}
	
	/**
	 * 
	 * @return a hashMap ready for dynamoDB update
	 */
	public HashMap<String, AttributeValueUpdate> toMapForUpdate() {
		HashMap<String, AttributeValueUpdate> item_values = new HashMap<String, AttributeValueUpdate>();
				
		if (bHSharedSecret != null && bHSharedSecret.length() > 0)
		    item_values.put(FIELDS.BHSharedSecret.fieldName, new AttributeValueUpdate(new AttributeValue(bHSharedSecret), AttributeAction.PUT));
		
		if (clientId != null && clientId.length() > 0)
		    item_values.put(FIELDS.Client_Id.fieldName, new AttributeValueUpdate(new AttributeValue(clientId), AttributeAction.PUT));
		
		if (codeHash != null && codeHash.length() > 0)
		    item_values.put(FIELDS.CodeHash.fieldName, new AttributeValueUpdate(new AttributeValue(codeHash), AttributeAction.PUT));
		
		if (iPWhiteList != null && iPWhiteList.length() > 0)
		    item_values.put(FIELDS.IP_Whitelist.fieldName, new AttributeValueUpdate(new AttributeValue(iPWhiteList), AttributeAction.PUT));
		
		if (partnerAllowedMethods != null && partnerAllowedMethods.length() > 0)
		    item_values.put(FIELDS.Partner_Allowed_Methods.fieldName, new AttributeValueUpdate(new AttributeValue(partnerAllowedMethods), AttributeAction.PUT));
		
		if (partnerId != null && partnerId.length() > 0)
		    item_values.put(FIELDS.Partner_Id.fieldName, new AttributeValueUpdate(new AttributeValue(partnerId), AttributeAction.PUT));
		
		if (resellerId != null && resellerId.length() > 0)
		    item_values.put(FIELDS.Reseller_Id.fieldName, new AttributeValueUpdate(new AttributeValue(resellerId), AttributeAction.PUT));
		
		if (schema != null && schema.length() > 0)
		    item_values.put(FIELDS.Schema.fieldName, new AttributeValueUpdate(new AttributeValue(schema), AttributeAction.PUT));
		
		if (cEPUser != null && cEPUser.length() > 0)
		    item_values.put(FIELDS.CEPUser.fieldName, new AttributeValueUpdate(new AttributeValue(cEPUser), AttributeAction.PUT));
		
		if (cEPPassword != null && cEPPassword.length() > 0)
		    item_values.put(FIELDS.CEPPassword.fieldName, new AttributeValueUpdate(new AttributeValue(cEPPassword), AttributeAction.PUT));
		
		return item_values;
	}
}
