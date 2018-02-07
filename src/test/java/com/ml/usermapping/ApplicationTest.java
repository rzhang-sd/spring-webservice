package com.ml.usermapping;

import static org.junit.Assert.*;

import java.util.HashMap;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.ml.usermapping.controller.UserController;

@RunWith(SpringRunner.class)
@SpringBootTest
@WebAppConfiguration
public class ApplicationTest {
	
	private MockMvc mvc;
	
	private TestRestTemplate _testTemplate;
	
	@Before
	public void setUp() throws Exception {
		mvc = MockMvcBuilders.standaloneSetup(new UserController()).build();
	}
	
	@Test
	public void testUserController() throws Exception {
		this._testTemplate = new TestRestTemplate();
		ResponseEntity<HashMap> responseEntity = this._testTemplate.getForEntity("http://localhost:8080/users/MyTest", HashMap.class);
		HashMap<String, Object> map = (HashMap<String, Object>) responseEntity.getBody();
		//System.out.println("Shared secret: " + responseEntity.getBody().toString());
		assertEquals(responseEntity.getStatusCode(), HttpStatus.OK);
	}

	@Test
	public void testPasswordEncryption() throws Exception {
		UserController user = new UserController();
		String passwd = "Testpassword";
		String encryptedPS = user.encryptPws(passwd);
		System.out.println("encrypted password: " + encryptedPS);
		String decryptedPS = user.decryptPws(encryptedPS);
		System.out.println("Decrypted password: " + decryptedPS);
		assertEquals(decryptedPS, passwd);
	}
}
