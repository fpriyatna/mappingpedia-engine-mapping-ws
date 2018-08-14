package es.upm.fi.dia.oeg.mappingpedia;

import static org.junit.Assert.*;

import org.json.JSONObject;
import org.junit.Test;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.request.HttpRequestWithBody;

public class MappingsWSControllerTest {

	@Test
	public void test() {
		String postMappingsUrl = "http://localhost:8094/mappings/test-mobileage-upm3/6ec7224b-7292-43e7-a8a3-e2050588c3e4";
		String mdDownloadUrl = "https://raw.githubusercontent.com/oeg-upm/morph-rdb/master/morph-examples/examples-srilanka-tourism/2016-P21.r2rml.ttl";
		String mdMappingLanguage = "r2rml";
		
		HttpRequestWithBody request = Unirest.post(postMappingsUrl);
		/*
		request.field("mapping_document_download_url", mdDownloadUrl);
		request.field("mapping_language", mdMappingLanguage);
		*/
		JSONObject jsonObj = new JSONObject();
		jsonObj.put("mapping_document_download_url", mdDownloadUrl);
		jsonObj.put("mappingDocumentDownloadURL", mdDownloadUrl);
		jsonObj.put("mapping_language", mdMappingLanguage);
		try {
			HttpResponse response = request
					.body(jsonObj)
					.asJson();
			assertTrue(response.getStatusText(), true);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
			
		}
		

		

		//fail("Not yet implemented");
	}

}
