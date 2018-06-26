package es.upm.fi.dia.oeg.mappingpedia;

//import org.apache.jena.ontology.OntModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.MultipartProperties;

@SpringBootApplication
public class MappingsApplication {


	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger("MappingsApplication");
		logger.info("Working Directory = " + System.getProperty("user.dir"));
		logger.info("Starting Mappingpedia Engine Mappings WS version 1.0.0 ...");

		/*
		InputStream is = null;
		String configurationFilename = "config.properties";
		try {

			logger.info("Loading configuration file ...");
			is = MappingsApplication.class.getClassLoader().getResourceAsStream(configurationFilename);
			if(is==null){
				logger.error("Sorry, unable to find " + configurationFilename);
				return;
			}
			MappingPediaProperties properties = new MappingPediaProperties(is);
			properties.load(is);
			logger.info("Configuration file loaded.");
			MappingPediaEngine.init(properties);
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally{
			if(is!=null){
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		*/

		SpringApplication.run(MappingsApplication.class, args);
		MultipartProperties multipartProperties = new MultipartProperties();
		multipartProperties.setLocation("./mpe-mappings-temp");
		String multiPartPropertiesLocation = multipartProperties.getLocation();
		logger.info("multiPartPropertiesLocation = " + multiPartPropertiesLocation);
		logger.info("Mappingpedia Engine Mappings WS started.\n\n\n");

	}
}
