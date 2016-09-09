Document Ingestion 
==================

Each record from each source to be ingested is represented by a JSON document in the MongoDB.
This JSON document contains the original document, the processing results, provenance and processing information used by the Foundry-ES message oriented document processing pipeline.

### Primary Key Representation

While each MongoDB ingested document is provided with a unique synthetic id, to identify the document a more natural primary key is also needed. 
The value of a field inside the JSON document can be used as the natural primary key. A pointer to that field is represented in JSONPath (similar to XPath) 
and stored in the source metadata JSON document in MongoDB (See above). 
A JSONPath processor is implemented by `org.neuinfo.foundry.common.util.JSONPathProcessor` class in `common` subproject.


## Document Wrapper JSON

The object model of the document wrapper JSON representation to wrap the original document and manage processing of the document until Elasticsearch indexing is defined in `common` subproject 
`org.neuinfo.foundry.common.model.DocWrapper` class. 

Below is the JSON representation of the document wrapper that is stored in MongoDB.

```JSON
{
   "primaryKey":"<>",
   "Version":"<version-number>",
   "CrawlDate":"",
   "SourceInfo": { 
      "SourceID":"<source-id of the source>",
      "ViewID":"",
      "Name":"<source-name>",
      "DataSource":"<data-source-name (default is sourceID)>"
   },   
   "OriginalDoc":"<original doc as JSON>",
   "Data": {
       "transformedRec":"<original doc transformed for indexing as JSON>"
   },
   "Processing": {
     "Status":"<process-status used to guide message oriented document processors>",
   },
   "History": {
    "provenance":"<provenance records in PROV-DM JSON format for the document ingestion and processing>",
    "batchId":"<>",
   }
}
```

## Provenance

PROV-DM specification (http://www.w3.org/TR/2013/REC-prov-dm-20130430/) based ProvToolBox (https://github.com/lucmoreau/ProvToolbox) libraries are used in foundry-common module to build a representation independent Java model of provenance and save is in PROV-JSON format (http://www.w3.org/Submission/2013/SUBM-prov-json-20130424/).

In `foundry-common module` there is a builder for provenance record hiding the details of JSON-DM. Below is an example, using Java fluent API style, to state that a document `doc1` entity is processed by  software agent `docIdAssigner` that assigns a document id to the entity. The activity took place at May 16 2014, 16:05 lasting 1 second.

```java
final ProvenanceRec.Builder builder = new ProvenanceRec.Builder("http://example.org", "foundry");

ProvenanceRec provenanceRec = builder.docEntity("doc1", "document")
                .softwareAgent("docIdAssigner")
                .activity("assignId", "doc-id-assigment",  "2014-05-16T16:05:00",
                        "2014-05-16T16:05:01")
                .wasAssociatedWith("assignId","docIdAssigner")
                .used("assignId","doc1")
                .build();

provenanceRec.save("/tmp/doc_id_assigment_prov.json");
```

Below is the PROV-JSON document generated and also validated against PROV-JSON JSON schema ()

```JSON
{
  "wasAssociatedWith": {
    "_:wAW2": {
      "prov:activity": "foundry:assignId",
      "prov:agent": "foundry:docIdAssigner"
    }
  },
  "entity": {
    "foundry:doc1": {
      "prov:label": "document"
    }
  },
  "prefix": {
    "xsd": "http://www.w3.org/2001/XMLSchema",
    "prov": "http://www.w3.org/ns/prov#",
    "foundry": "http://example.org"
  },
  "used": {
    "_:u2": {
      "prov:activity": "foundry:assignId",
      "prov:entity": "foundry:doc1"
    }
  },
  "agent": {
    "foundry:docIdAssigner": {
      "prov:type": {
        "$": "prov:SoftwareAgent",
        "type": "xsd:string"
      }
    }
  },
  "activity": {
    "foundry:assignId": {
      "prov:type": {
        "$": "doc-id-assigment",
        "type": "xsd:string"
      },
      "prov:startTime": "2014-05-16T16:05:00-07:00",
      "prov:endTime": "2014-05-16T16:05:01-07:00"
    }
  }
}
```


