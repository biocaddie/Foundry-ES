Harvest Descriptors
------------------

## A 

```JSON
{
    "SourceInformation": {
      "ResourceID": "nlx_152590",
      "Name": "Open Source Brain",
      "DataSource": "OSB Projects"
    },
    
    "IngestConfiguration": {
      "IngestMethod": "XML",
      "IngestURL": "http://www.opensourcebrain.org/projects.xml?limit=1000",
      "allowDuplicates": "False",
      
      "CrawlFrequency": {
        "CrawlType": "Frequency",
        "Hours": "48",
        "Minutes": "0",
        "StartDays": ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"],
        "StartTime": "0:00",
        "OperationEndTime": "24:00" 
      }  
    },
        
    "ContentSpecification": {
      "KeepMissing": "false",
      "TopElement": "projects",
      "DocumentElement": "project",
      "Locale": "en_US"
    },
    
    "PrimaryKey": {
      "Fields": ["$..id.'_$'"],
      "Delimiter": [":"],
      "Method": "Value"
    },
      
    "DocumentProcessing": ["UUID Generation", "Index"]     
}
```


