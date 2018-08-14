package es.upm.fi.dia.oeg.mappingpedia;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.annotation.MultipartConfig;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingDocumentController;
import es.upm.fi.dia.oeg.mappingpedia.model.*;
import es.upm.fi.dia.oeg.mappingpedia.model.result.*;
//import es.upm.fi.dia.oeg.mappingpedia.utility.*;
import es.upm.fi.dia.oeg.mappingpedia.utility.MpcCkanUtility;
import es.upm.fi.dia.oeg.mappingpedia.utility.MpcUtility;
import org.apache.commons.io.FileUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
//@RequestMapping(value = "/mappingpedia")
@MultipartConfig(fileSizeThreshold = 20971520)
public class MappingsWSController {
    //static Logger logger = LogManager.getLogger("MappingsWSController");
    static Logger logger = LoggerFactory.getLogger("MappingsWSController");

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();


    //private OntModel ontModel = MappingPediaEngine.ontologyModel();


    private MappingDocumentController mappingDocumentController = MappingDocumentController.apply();

    /*
    @RequestMapping(value="/greeting", method= RequestMethod.GET)
    public GreetingJava getGreeting(@RequestParam(value="name", defaultValue="World") String name) {
        logger.info("/greeting(GET) ...");
        return new GreetingJava(counter.incrementAndGet(),
                String.format(template, name));
    }
    */

    @RequestMapping(value="/", method= RequestMethod.GET, produces={"application/ld+json"})
    public Inbox get() {
        logger.info("GET / ...");
        return new Inbox();
    }

    @RequestMapping(value="/", method= RequestMethod.HEAD, produces={"application/ld+json"})
    public ResponseEntity head() {
        logger.info("HEAD / ...");
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.LINK, "<http://mappingpedia-engine.linkeddata.es/inbox>; rel=\"http://www.w3.org/ns/ldp#inbox\"");

        return new ResponseEntity(headers, HttpStatus.CREATED);
    }

    @RequestMapping(value="/inbox", method= RequestMethod.POST)
    public GeneralResult postInbox(
            //@RequestParam(value="notification", required = false) Object notification)
            @RequestBody Object notification
    )
    {
        logger.info("POST /inbox ...");
        logger.info("notification = " + notification);
        return new GeneralResult(HttpStatus.OK.getReasonPhrase(), HttpStatus.OK.value());
    }

    @RequestMapping(value="/inbox", method= RequestMethod.PUT)
    public GeneralResult putInbox(
            //@RequestParam(value="notification", defaultValue="") String notification
            @RequestBody Object notification
    )
    {
        logger.info("PUT /inbox ...");
        logger.info("notification = " + notification);
        return new GeneralResult(HttpStatus.OK.getReasonPhrase(), HttpStatus.OK.value());
    }




    /*
    @RequestMapping(value="/greeting/{name}", method= RequestMethod.PUT)
    public GreetingJava putGreeting(@PathVariable("name") String name) {
        logger.info("/greeting(PUT) ...");
        return new GreetingJava(counter.incrementAndGet(),
                String.format(template, name));
    }
    */




    @RequestMapping(value="/github_repo_url", method= RequestMethod.GET)
    public String getGitHubRepoURL() {
        logger.info("GET /github_repo_url ...");
        return MappingPediaEngine.mappingpediaProperties().githubRepository();
    }

    @RequestMapping(value="/ckan_datasets", method= RequestMethod.GET)
    public ListResult getCKANDatasets(@RequestParam(value="catalogUrl", required = false) String catalogUrl) {
        if(catalogUrl == null) {
            catalogUrl = MappingPediaEngine.mappingpediaProperties().ckanURL();
        }
        logger.info("GET /ckanDatasetList ...");
        return MpcCkanUtility.getDatasetList(catalogUrl);
    }

    @RequestMapping(value="/virtuoso_enabled", method= RequestMethod.GET)
    public String getVirtuosoEnabled() {
        logger.info("GET /virtuosoEnabled ...");
        return MappingPediaEngine.mappingpediaProperties().virtuosoEnabled() + "";
    }

    @RequestMapping(value="/mappingpedia_graph", method= RequestMethod.GET)
    public String getMappingpediaGraph() {
        logger.info("/getMappingPediaGraph(GET) ...");
        return MappingPediaEngine.mappingpediaProperties().graphName();
    }

    @RequestMapping(value="/ckan_api_action_organization_create", method= RequestMethod.GET)
    public String getCKANAPIActionOrganizationCreate() {
        logger.info("GET /ckanActionOrganizationCreate ...");
        return MappingPediaEngine.mappingpediaProperties().ckanActionOrganizationCreate();
    }

    @RequestMapping(value="/ckan_api_action_package_create", method= RequestMethod.GET)
    public String getCKANAPIActionPpackageCreate() {
        logger.info("GET /ckanActionPackageCreate ...");
        return MappingPediaEngine.mappingpediaProperties().ckanActionPackageCreate();
    }

    @RequestMapping(value="/ckan_api_action_resource_create", method= RequestMethod.GET)
    public String getCKANAPIActionResourceCreate() {
        logger.info("GET /getCKANActionResourceCreate ...");
        return MappingPediaEngine.mappingpediaProperties().ckanActionResourceCreate();
    }



    @RequestMapping(value="/ckanResource", method= RequestMethod.POST)
    public Integer postCKANResource(
            @RequestParam(value="filePath", required = true) String filePath
            , @RequestParam(value="packageId", required = true) String packageId
    ) {
        logger.info("POST /ckanResource...");
        String ckanURL = MappingPediaEngine.mappingpediaProperties().ckanURL();
        String ckanKey = MappingPediaEngine.mappingpediaProperties().ckanKey();

        MpcCkanUtility ckanClient = new MpcCkanUtility(ckanURL, ckanKey);
        File file = new File(filePath);
        try {
            if(!file.exists()) {
                String fileName = file.getName();
                file = new File(fileName);
                FileUtils.copyURLToFile(new URL(filePath), file);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

        //return ckanUtility.createResource(file.getPath(), packageId);
        return null;
    }

    @RequestMapping(value="/dataset_language/{organizationId}", method= RequestMethod.POST)
    public Integer postDatasetLanguage(
            @PathVariable("organizationId") String organizationId
            , @RequestParam(value="dataset_language", required = true) String datasetLanguage
    ) {
        logger.info("POST /dataset_language ...");
        String ckanURL = MappingPediaEngine.mappingpediaProperties().ckanURL();
        String ckanKey = MappingPediaEngine.mappingpediaProperties().ckanKey();

        MpcCkanUtility ckanClient = new MpcCkanUtility(ckanURL, ckanKey);
        return ckanClient.updateDatasetLanguage(organizationId, datasetLanguage);
    }

    @RequestMapping(value="/triples_maps", method= RequestMethod.GET)
    public ListResult getTriplesMaps() {
        logger.info("/triplesMaps ...");
        ListResult listResult = MappingPediaEngine.getAllTriplesMaps();
        //logger.info("listResult = " + listResult);

        return listResult;
    }

    @RequestMapping(value="/mappings", method= RequestMethod.GET)
    public ListResult getMappings(
            @RequestParam(value="id", defaultValue = "", required = false) String mdId
            , @RequestParam(value="dataset_id", defaultValue = "", required = false) String datasetId
            , @RequestParam(value="ckan_package_id", defaultValue = "", required = false) String ckanPackageId
            , @RequestParam(value="ckan_package_name", defaultValue = "", required = false) String ckanPackageName
            , @RequestParam(value="distribution_id", defaultValue = "", required = false) String distributionId
    ) {
        logger.info("GET /mappings");
        logger.info("id = " + mdId);
        logger.info("dataset_id = " + datasetId);
        logger.info("ckan_package_id = " + ckanPackageId);
        logger.info("ckan_package_name = " + ckanPackageName);
        logger.info("distribution_id = " + distributionId);

        ListResult<MappingDocument> listResult = null;

        if(!"".equals(mdId.trim())) {
            listResult = this.mappingDocumentController.findById(mdId);
        } else if(!"".equals(datasetId.trim())) {
            listResult = this.mappingDocumentController.findByDatasetId(datasetId, ckanPackageId, ckanPackageName);
        } else if(!"".equals(ckanPackageId.trim())) {
            listResult = this.mappingDocumentController.findByCKANPackageId(ckanPackageId);
        } else if(!"".equalsIgnoreCase(distributionId.trim())) {
            listResult = this.mappingDocumentController.findByDistributionId(distributionId);
        } else {

        }

        return listResult;
    }




    @RequestMapping(value="/mapped_classes", method= RequestMethod.GET)
    public ListResult getMappedClasses(@RequestParam(value="prefix", required = false, defaultValue="schema.org") String prefix
            , @RequestParam(value="mapped_table", required = false) String mappedTable
            , @RequestParam(value="mapping_document_id", required = false) String mappingDocumentId
    ) {
        logger.info("/mapped_classes ...");
        logger.info("prefix = " + prefix);
        ListResult listResult = null;
        if(mappingDocumentId != null) {
            listResult = this.mappingDocumentController.findMappedClassesByMappingDocumentId(mappingDocumentId);
        } else if(mappedTable != null) {
            listResult = this.mappingDocumentController.findAllMappedClassesByTableName(prefix, mappedTable);
        } else {
            listResult = this.mappingDocumentController.findAllMappedClasses(prefix);
        }

        logger.info("mapped_classes result = " + listResult);

        return listResult;
    }

    @RequestMapping(value="/mapped_properties", method= RequestMethod.GET)
    public ListResult getMappedProperty(@RequestParam(value="prefix", required = false, defaultValue="schema.org") String prefix
    ) {
        logger.info("/mapped_properties ...");
        logger.info("prefix = " + prefix);
        ListResult listResult = this.mappingDocumentController.findAllMappedProperties(prefix);
        logger.info("mapped_properties result = " + listResult);

        return listResult;
    }

    @RequestMapping(value="/ogd/annotations", method= RequestMethod.GET)
    public ListResult getOGDAnnotations(
            //@RequestParam(value="searchType", defaultValue = "0") String searchType,
            @RequestParam(value="class", required = false) String searchedClass
            , @RequestParam(value="property", required = false) String searchedProperty
            , @RequestParam(value="subclass", required = false, defaultValue="true") String subclass

    ) {
        logger.info("/ogd/annotations(GET) ...");
        logger.info("searchedClass = " + searchedClass);
        logger.info("searchedProperty = " + searchedProperty);

        if("true".equalsIgnoreCase(subclass)) {
            logger.info("get all mapping documents by mapped class and its subclasses ...");
/*            ListResult listResult = this.mappingDocumentController.findMappingDocumentsByMappedClass(
                    searchClass, true);*/
            ListResult listResult = this.mappingDocumentController.findByClassAndProperty(
                    searchedClass, searchedProperty, true);

            //logger.info("listResult = " + listResult);
            return listResult;
        } else {
            //ListResult listResult = this.mappingDocumentController.findMappingDocuments(searchType, searchTerm);
            ListResult listResult = this.mappingDocumentController.findByClass(searchedClass);

            //logger.info("listResult = " + listResult);
            return listResult;
        }

    }



/*    //TODO REFACTOR THIS; MERGE /executions with /executions2
    //@RequestMapping(value="/executions1/{organizationId}/{datasetId}/{mappingFilename:.+}"
//            , method= RequestMethod.POST)
    @RequestMapping(value="/executions1/{organizationId}/{datasetId}/{mappingDocumentId}"
            , method= RequestMethod.POST)
    public ExecuteMappingResult postExecutions1(
            @PathVariable("organization_id") String organizationId

            , @PathVariable("dataset_id") String datasetId
            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL
            , @RequestParam(value="distribution_mediatype", required = false, defaultValue="text/csv") String distributionMediaType
            , @RequestParam(value="field_separator", required = false) String fieldSeparator

            , @RequestParam(value="mapping_document_id", required = false) String mappingDocumentId
            , @RequestParam(value="mapping_document_download_url", required = false) String mappingDocumentDownloadURL
            , @RequestParam(value="mapping_language", required = false) String pMappingLanguage

            , @RequestParam(value="query_file", required = false) String queryFile
            , @RequestParam(value="output_filename", required = false) String outputFilename

            , @RequestParam(value="db_username", required = false) String dbUserName
            , @RequestParam(value="db_password", required = false) String dbPassword
            , @RequestParam(value="db_name", required = false) String dbName
            , @RequestParam(value="jdbc_url", required = false) String jdbc_url
            , @RequestParam(value="database_driver", required = false) String databaseDriver
            , @RequestParam(value="database_type", required = false) String databaseType

            , @RequestParam(value="use_cache", required = false) String pUseCache
            //, @PathVariable("mappingFilename") String mappingFilename
    )
    {
        logger.info("POST /executions1/{organizationId}/{datasetId}/{mappingDocumentId}");
        logger.info("mapping_document_id = " + mappingDocumentId);

        Agent organization = new Agent(organizationId);

        Dataset dataset = new Dataset(organization, datasetId);
        Distribution distribution = new Distribution(dataset);
        if(distributionAccessURL != null) {
            distribution.dcatAccessURL_$eq(distributionAccessURL);
        }
        if(distributionDownloadURL != null) {
            distribution.dcatDownloadURL_$eq(distributionDownloadURL);
        } else {
            distribution.dcatDownloadURL_$eq(this.githubClient.getDownloadURL(distributionAccessURL));
        }
        if(fieldSeparator != null) {
            distribution.cvsFieldSeparator_$eq(fieldSeparator);
        }
        distribution.dcatMediaType_$eq(distributionMediaType);
        dataset.addDistribution(distribution);


        MappingDocument md = new MappingDocument();
        if(mappingDocumentDownloadURL != null) {
            md.setDownloadURL(mappingDocumentDownloadURL);
        } else {
            if(mappingDocumentId != null) {
                MappingDocument foundMappingDocument = this.mappingDocumentController.findMappingDocumentsByMappingDocumentId(mappingDocumentId);
                md.setDownloadURL(foundMappingDocument.getDownloadURL());
            } else {
                //I don't know that to do here, Ahmad will handle
            }
        }

        if(pMappingLanguage != null) {
            md.mappingLanguage_$eq(pMappingLanguage);
        } else {
            String mappingLanguage = MappingDocumentController.detectMappingLanguage(mappingDocumentDownloadURL);
            logger.info("mappingLanguage = " + mappingLanguage);
            md.mappingLanguage_$eq(mappingLanguage);
        }


        JDBCConnection jdbcConnection = new JDBCConnection(dbUserName, dbPassword
                , dbName, jdbc_url
                , databaseDriver, databaseType);


        Boolean useCache = MappingPediaUtility.stringToBoolean(pUseCache);
        try {
            //IN THIS PARTICULAR CASE WE HAVE TO STORE THE EXECUTION RESULT ON CKAN
            return mappingExecutionController.executeMapping(md, dataset, queryFile, outputFilename
                    , true, true, true, jdbcConnection
                    , useCache

            );
        } catch (Exception e) {
            e.printStackTrace();
            String errorMessage = "Error occured: " + e.getMessage();
            logger.error("mapping execution failed: " + errorMessage);
            ExecuteMappingResult executeMappingResult = new ExecuteMappingResult(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error"
                    , null, null
                    , null
                    , null, null
                    , null
                    , null
                    , null, null
            );
            return executeMappingResult;
        }
    }*/

    @RequestMapping(value = "/mappings/{organization_id}", method= RequestMethod.POST)
    public AddMappingDocumentResult postMappingsWithoutDataset(
            @PathVariable("organization_id") String organizationID

            , @RequestParam(value="dataset_id", required = false) String pDatasetID
            , @RequestParam(value="ckan_package_id", required = false) String ckanPackageId
            , @RequestParam(value="ckan_package_name", required = false) String ckanPackageName

            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="mappingFile", required = false) MultipartFile mappingFileMultipartFile
            , @RequestParam(value="mapping_document_file", required = false) MultipartFile mappingDocumentFileMultipartFile
            , @RequestParam(value="mapping_document_download_url", required = false) String pMappingDocumentDownloadURL1
            , @RequestParam(value="mappingDocumentDownloadURL", required = false) String pMappingDocumentDownloadURL2
            , @RequestParam(value="replaceMappingBaseURI", defaultValue="true") String replaceMappingBaseURI
            , @RequestParam(value="generateManifestFile", defaultValue="true") String generateManifestFile
            , @RequestParam(value="mappingDocumentTitle", defaultValue="") String mappingDocumentTitle
            , @RequestParam(value="mappingDocumentCreator", defaultValue="") String mappingDocumentCreator
            , @RequestParam(value="mappingDocumentSubjects", defaultValue="") String mappingDocumentSubjects
            , @RequestParam(value="mapping_language", required = false) String pMappingLanguage1
            , @RequestParam(value="mappingLanguage", required = false) String pMappingLanguage2

            , @RequestParam(value="ckan_resource_id", required = false, defaultValue="") String ckanResourceId
    )
    {
        logger.info("[POST] /mappings/{organization_id}");
        logger.info("organization_id = " + organizationID);
        logger.info("dataset_id = " + pDatasetID);
        logger.info("ckan_package_id = " + ckanPackageId);
        logger.info("ckan_package_name = " + ckanPackageName);
        logger.info("mappingFile = " + mappingFileMultipartFile);
        logger.info("mapping_document_file = " + mappingDocumentFileMultipartFile);
        logger.info("mapping_document_download_url = " + pMappingDocumentDownloadURL1);


        try {
            //FIND A DATASET IF DATASET ID IS NOT PROVIDED
            String datasetId;
            if(pDatasetID != null) {
                datasetId = pDatasetID;
            } else {
                String datasetsServerUrl = MPCConstants.ENGINE_DATASETS_SERVER() + "datasets";
                if(ckanPackageId != null && ckanPackageName == null) {
                    datasetsServerUrl += "?ckan_package_id=" + ckanPackageId;
                } else if(ckanPackageName != null && ckanPackageId  == null) {
                    datasetsServerUrl += "?ckan_package_name=" + ckanPackageName;
                }

                logger.info("datasetsServerUrl = " + datasetsServerUrl);
                HttpResponse<JsonNode> jsonResponse = Unirest.get(datasetsServerUrl)
                        .asJson();
                int responseStatus = jsonResponse.getStatus();
                if(responseStatus >= 200 && responseStatus < 300) {
                    datasetId = jsonResponse.getBody().getObject().getJSONArray("results").getJSONObject(0).getString("id");
                } else {
                    datasetId = null;
                }

            }
            logger.info("datasetId = " + datasetId);

            /*
            Dataset dataset = this.datasetController.findOrCreate(
                    organizationID, pDatasetID, ckanPackageId, ckanPackageName);
            String datasetId = dataset.dctIdentifier();
            */

            return this.postMappingsWithDataset(organizationID
                    , datasetId, ckanPackageId, ckanPackageName
                    , manifestFileRef
                    , mappingFileMultipartFile, mappingDocumentFileMultipartFile, pMappingDocumentDownloadURL1, pMappingDocumentDownloadURL2
                    , replaceMappingBaseURI, generateManifestFile
                    , mappingDocumentTitle, mappingDocumentCreator, mappingDocumentSubjects
                    , pMappingLanguage1, pMappingLanguage2
                    , ckanResourceId);

        } catch (Exception e) {
            e.printStackTrace();
            return new AddMappingDocumentResult(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()
                    , null
                    , null, null
            );
        }
    }


    @RequestMapping(value = "/mappings/{organization_id}/{dataset_id}", method= RequestMethod.POST)
    public AddMappingDocumentResult postMappingsWithDataset(
            @PathVariable("organization_id") String organizationID

            , @PathVariable("dataset_id") String datasetID
            , @RequestParam(value="ckan_package_id", required = false) String ckanPackageId
            , @RequestParam(value="ckan_package_name", required = false) String ckanPackageName

            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef

            , @RequestParam(value="mappingFile", required = false) MultipartFile mappingFileMultipartFile
            , @RequestParam(value="mapping_document_file", required = false) MultipartFile mappingDocumentFileMultipartFile
            , @RequestParam(value="mapping_document_download_url", required = false) String pMappingDocumentDownloadURL1
            , @RequestParam(value="mappingDocumentDownloadURL", required = false) String pMappingDocumentDownloadURL2

            , @RequestParam(value="replaceMappingBaseURI", defaultValue="true") String replaceMappingBaseURI
            , @RequestParam(value="generateManifestFile", defaultValue="true") String pGenerateManifestFile
            , @RequestParam(value="mappingDocumentTitle", defaultValue="") String mappingDocumentTitle
            , @RequestParam(value="mappingDocumentCreator", defaultValue="") String mappingDocumentCreator
            , @RequestParam(value="mappingDocumentSubjects", defaultValue="") String mappingDocumentSubjects
            , @RequestParam(value="mapping_language", required = false) String pMappingLanguage1
            , @RequestParam(value="mappingLanguage", required = false) String pMappingLanguage2

            , @RequestParam(value="ckan_resource_id", required = false, defaultValue="") String ckanResourceId
    )
    {
        logger.info("[POST] /mappings/{organization_id}/{dataset_id}");
        logger.info("mapping_document_download_url = " + pMappingDocumentDownloadURL1);
        logger.info("mappingDocumentDownloadURL = " + pMappingDocumentDownloadURL2);
        logger.info("organization_id = " + organizationID);
        logger.info("dataset_id = " + datasetID);
        logger.info("ckan_package_id = " + ckanPackageId);
        logger.info("ckan_package_name = " + ckanPackageName);
        logger.info("mapping_language = " + pMappingLanguage1);
        logger.info("mappingLanguage = " + pMappingLanguage2);
        try {
            boolean generateManifestFile = MpcUtility.stringToBoolean(pGenerateManifestFile);
            File manifestFile = MpcUtility.multipartFileToFile(manifestFileRef, datasetID);

            MappingDocument mappingDocument = new MappingDocument();
            mappingDocument.ckanPackageId_$eq(ckanPackageId);
            mappingDocument.ckanResourceId_$eq(ckanResourceId);
            mappingDocument.dctSubject_$eq(mappingDocumentSubjects);
            mappingDocument.dctCreator_$eq(mappingDocumentCreator);
            mappingDocument.setTitle(mappingDocumentTitle, mappingDocument.dctIdentifier());
            mappingDocument.setMappingLanguage(pMappingLanguage1, pMappingLanguage2);

            mappingDocument.setFile(mappingDocumentFileMultipartFile, mappingFileMultipartFile
                    , datasetID);
            if(mappingDocument.mappingLanguage() == null && mappingDocument.mappingDocumentFile() != null) {
                String inferredMappingLanguage = MappingDocumentController.detectMappingLanguage(
                        mappingDocument.mappingDocumentFile());
                mappingDocument.mappingLanguage_$eq(inferredMappingLanguage);
            }

            mappingDocument.setDownloadURL(pMappingDocumentDownloadURL1, pMappingDocumentDownloadURL2);
            if(mappingDocument.mappingLanguage() == null && mappingDocument.getDownloadURL() != null) {
                String inferredMappingLanguage = MappingDocumentController.detectMappingLanguage(
                        mappingDocument.getDownloadURL());
                mappingDocument.mappingLanguage_$eq(inferredMappingLanguage);
            }



            logger.info("mappingDocument.mappingLanguage() = " + mappingDocument.mappingLanguage());
            return mappingDocumentController.addNewMappingDocument(
                    organizationID
                    , datasetID
                    , ckanPackageId
                    , manifestFile
                    , replaceMappingBaseURI
                    , generateManifestFile
                    , mappingDocument
            );
        } catch (Exception e) {
            e.printStackTrace();

            return new AddMappingDocumentResult(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()
                    , null
                    , null, null
            );
        }

    }

    @RequestMapping(value="/mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename:.+}", method= RequestMethod.GET)
    public GeneralResult getMapping(
            @PathVariable("mappingpediaUsername") String mappingpediaUsername
            , @PathVariable("mappingDirectory") String mappingDirectory
            , @PathVariable("mappingFilename") String mappingFilename
    )
    {
        logger.info("GET /mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}");
        return MappingPediaEngine.getMapping(mappingpediaUsername, mappingDirectory, mappingFilename);
    }

    @RequestMapping(value="/mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename:.+}", method= RequestMethod.PUT)
    public GeneralResult putMappings(
            @PathVariable("mappingpediaUsername") String mappingpediaUsername
            , @PathVariable("mappingDirectory") String mappingDirectory
            , @PathVariable("mappingFilename") String mappingFilename
            , @RequestParam(value="mappingFile") MultipartFile mappingFileRef
    )
    {
        logger.info("PUT /mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}");

        File mappingFile = MpcUtility.multipartFileToFile(mappingFileRef, mappingDirectory);
        return mappingDocumentController.updateExistingMapping(mappingpediaUsername, mappingDirectory, mappingFilename
                , mappingFile);
    }




    @RequestMapping(value = "/queries/{mappingpediaUsername}/{datasetID}", method= RequestMethod.POST)
    public GeneralResult postQueries(
            @RequestParam("queryFile") MultipartFile queryFileRef
            , @PathVariable("mappingpediaUsername") String mappingpediaUsername
            , @PathVariable("datasetID") String datasetID
    )
    {
        logger.info("[POST] /queries/{mappingpediaUsername}/{datasetID}");
        return MappingPediaEngine.addQueryFile(queryFileRef, mappingpediaUsername, datasetID);
    }


    @RequestMapping(value = "/rdf_file", method= RequestMethod.POST)
    public GeneralResult postRDFFile(
            @RequestParam("rdfFile") MultipartFile fileRef
            , @RequestParam(value="graphURI") String graphURI)
    {
        logger.info("/storeRDFFile...");
        return MappingPediaEngine.storeRDFFile(fileRef, graphURI);
    }

    @RequestMapping(value="/ogd/utility/subclasses", method= RequestMethod.GET)
    public ListResult getSubclassesDetails(
            @RequestParam(value="aClass") String aClass
    ) {
        logger.info("GET /ogd/utility/subclasses ...");
        logger.info("aClass = " + aClass);
        ListResult result = MappingPediaEngine.getSchemaOrgSubclassesDetail(aClass) ;
        //logger.info("result = " + result);
        return result;
    }

    @RequestMapping(value="/ogd/utility/subclassesSummary", method= RequestMethod.GET)
    public ListResult getSubclassesSummary(
            @RequestParam(value="aClass") String aClass
    ) {
        logger.info("GET /ogd/utility/subclassesSummary ...");
        logger.info("aClass = " + aClass);
        ListResult result = MappingPediaEngine.getSubclassesSummary(aClass) ;
        //logger.info("result = " + result);
        return result;
    }



}