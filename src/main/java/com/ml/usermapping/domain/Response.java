package com.ml.usermapping.domain;

import java.util.ArrayList;
import java.util.List;

public class Response {
	
	private BHCEPMapping results = null;
	public BHCEPMapping getResults() {
		return results;
	}

	public void setResults(BHCEPMapping results) {
		this.results = results;
	}

	public List<String> getInfo() {
		return info;
	}

	public void setInfo(List<String> info) {
		this.info = info;
	}

	List<String> info;
	
	public Response(BHCEPMapping ret, String message) {
		if (ret != null)
		    results = ret;
		info = new ArrayList<String>();
		if (message.length() > 0)
		    info.add(message);
	}
	
	
}
